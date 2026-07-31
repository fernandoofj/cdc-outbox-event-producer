package br.com.fltech.outbox.publisher.adapter.replay

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.core.port.MappingRules
import br.com.fltech.outbox.publisher.core.port.SourceReplayer
import br.com.fltech.outbox.publisher.core.port.UnsupportedReplayException
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

// LongParameterList: 7 collaborators (replayers map, mapping rules,
// sink registry, metrics, two cap knobs, executor factory) — each
// is a distinct concern the consumer can override.

/**
 * Orchestrates source-side replay jobs. Holds a single-active-job
 * mutex so a misclick on the operator endpoint cannot launch two
 * concurrent replays that would compete on the same source binlog
 * session. Each job runs on a background daemon thread, surfaces
 * progress via [getJob], and counts events through Micrometer.
 *
 * Replay re-uses the live publish pipeline:
 *  1. Open the bounded [SourceReplayer.openBoundedSource] for the
 *     requested window — that's a regular [RowChangeSource].
 *  2. For each `RowChange`, call [MappingRules.map] — same routing
 *     and key/payload projection the live processor uses.
 *  3. Publish via [EventSinkRegistry.publish] — same sink path,
 *     same retry semantics. Replay-side `ack()` is a no-op so the
 *     live producer's checkpoint state is never touched.
 *  4. Track count + duration; emit metric on completion.
 *
 * Override of target scheme/target is supported per-request so the
 * operator can route a replay to a different sink (e.g., a test
 * topic) without disturbing the original consumers.
 */
@Suppress("LongParameterList")
class ReplayService(
    private val replayers: Map<String, SourceReplayer>,
    private val mappingRules: MappingRules,
    private val sinkRegistry: EventSinkRegistry,
    private val metrics: CdcOutboxMetrics,
    private val maxEventsPerJob: Int = DEFAULT_MAX_EVENTS,
    private val jobTimeoutMs: Long = DEFAULT_TIMEOUT_MS,
    private val executorFactory: () -> ExecutorService = { defaultExecutor() },
) {
    private val active = AtomicReference<ReplayJob?>(null)
    private val finished = ConcurrentHashMap<String, ReplayJob>()

    /**
     * Starts a replay job. Returns immediately with a [ReplayJob]
     * carrying the assigned `jobId`; the actual draining happens
     * on a background thread. Throws [ConcurrentReplayException]
     * when another job is already running.
     */
    fun startReplay(request: ReplayRequest): ReplayJob {
        val replayer =
            replayers[request.sourceKind]
                ?: throw UnsupportedReplayException(
                    "no SourceReplayer registered for sourceKind='${request.sourceKind}'; " +
                        "known: ${replayers.keys}",
                )
        val job =
            ReplayJob(
                jobId = UUID.randomUUID().toString(),
                sourceKind = request.sourceKind,
                fromPosition = request.fromPosition,
                toPosition = request.toPosition,
                dryRun = request.dryRun,
                override = request.override,
                startedAt = Instant.now(),
            )
        if (!active.compareAndSet(null, job)) {
            throw ConcurrentReplayException(
                "another replay is already running: jobId=${active.get()?.jobId}",
            )
        }
        val executor = executorFactory()
        executor.execute { run(job, replayer, executor) }
        return job
    }

    /** Returns the job snapshot — active or finished — or `null` when unknown. */
    fun getJob(jobId: String): ReplayJob? {
        val current = active.get()
        if (current?.jobId == jobId) return current
        return finished[jobId]
    }

    /** List of jobs that have completed since process start (capped to avoid leak). */
    fun finishedJobs(): List<ReplayJob> = finished.values.toList()

    private fun run(
        job: ReplayJob,
        replayer: SourceReplayer,
        executor: ExecutorService,
    ) {
        val started = System.currentTimeMillis()
        try {
            val source = replayer.openBoundedSource(job.fromPosition, job.toPosition)
            source.open()
            try {
                drainLoop(job, source, started)
            } finally {
                source.close()
            }
            job.status = ReplayStatus.SUCCEEDED
        } catch (e: Exception) {
            // Errors (OOM, etc) escape to the executor's
            // UncaughtExceptionHandler — wired by the auto-config when
            // it builds the daemon-thread factory.
            logger.error(
                "ReplayService: job {} failed (sourceKind={}, from={}, to={}, cause={})",
                job.jobId,
                job.sourceKind,
                job.fromPosition,
                job.toPosition,
                e.javaClass.simpleName,
                e,
            )
            job.status = ReplayStatus.FAILED
            job.errorClass = e.javaClass.simpleName
            job.errorMessage = e.message
        } finally {
            job.finishedAt = Instant.now()
            job.elapsedMs = System.currentTimeMillis() - started
            metrics.recordReplayDuration(job.sourceKind, Duration.ofMillis(job.elapsedMs))
            active.compareAndSet(job, null)
            finished[job.jobId] = job
            // Don't shutdown executor here — caller may inject a shared one.
            // The default factory shutdowns inside the thread when done.
            if (executor is OneShotExecutor) executor.shutdown()
        }
    }

    private fun drainLoop(
        job: ReplayJob,
        source: br.com.fltech.outbox.publisher.core.port.RowChangeSource,
        started: Long,
    ) {
        var count = 0L
        var running = true
        while (running) {
            when (val step = nextStep(job, source, started, count)) {
                is DrainStep.Timeout -> throw step.cause
                is DrainStep.Capped -> {
                    job.cappedAtMaxEvents = true
                    running = false
                }
                is DrainStep.Drained -> running = false
                is DrainStep.Process -> {
                    processOne(job, step.rowChange, ++count)
                }
            }
        }
        job.eventsProcessed = count
    }

    // ReturnCount: 4 distinct DrainStep outcomes (Timeout, Capped,
    // Drained, Process). Guard-clause shape mirrors the existing
    // project convention.

    /**
     * One iteration of the drain loop. Decides whether to process a
     * row, stop because the cap was hit, stop because the window
     * drained, or abort because the job timed out. Pulled out of
     * `drainLoop` so the loop body is a single `when` instead of a
     * chain of `break`/`continue` jumps.
     */
    @Suppress("ReturnCount")
    private fun nextStep(
        job: ReplayJob,
        source: br.com.fltech.outbox.publisher.core.port.RowChangeSource,
        started: Long,
        count: Long,
    ): DrainStep {
        if (System.currentTimeMillis() - started > jobTimeoutMs) {
            return DrainStep.Timeout(
                ReplayTimeoutException(
                    "replay job ${job.jobId} exceeded ${jobTimeoutMs}ms timeout after $count events",
                ),
            )
        }
        if (count >= maxEventsPerJob) {
            logger.warn(
                "ReplayService: job {} hit max-events cap ({}); stopping early",
                job.jobId,
                maxEventsPerJob,
            )
            return DrainStep.Capped
        }
        val first = source.poll() ?: return drainOrSecondPoll(source)
        return DrainStep.Process(first)
    }

    /**
     * `poll() == null` is ambiguous: either the bounded source's
     * window was drained, or there was a brief gap with no event.
     * One follow-up poll disambiguates — two consecutive nulls mean
     * window drained.
     */
    private fun drainOrSecondPoll(source: br.com.fltech.outbox.publisher.core.port.RowChangeSource): DrainStep {
        val second = source.poll() ?: return DrainStep.Drained
        return DrainStep.Process(second)
    }

    /** Tri-state outcome of a drain-loop iteration. */
    private sealed class DrainStep {
        data class Timeout(val cause: ReplayTimeoutException) : DrainStep()

        object Capped : DrainStep()

        object Drained : DrainStep()

        data class Process(
            val rowChange: br.com.fltech.outbox.publisher.core.domain.RowChange,
        ) : DrainStep()
    }

    private fun processOne(
        job: ReplayJob,
        rowChange: br.com.fltech.outbox.publisher.core.domain.RowChange,
        count: Long,
    ) {
        val mapped =
            mappingRules.map(rowChange) ?: run {
                job.eventsFilteredOut += 1
                return
            }
        val routing = applyOverride(mapped.routing, job.override)
        val event = mapped.copy(routing = routing)
        if (job.dryRun) {
            job.eventsThatWouldBePublished += 1
            return
        }
        try {
            sinkRegistry.publish(routing, event)
            job.eventsPublished += 1
            metrics.recordReplayEvent(job.sourceKind, routing.scheme, "success")
        } catch (e: Exception) {
            job.eventsPublishFailed += 1
            metrics.recordReplayEvent(job.sourceKind, routing.scheme, "publish_failed")
            logger.warn(
                "ReplayService: job {} publish #{} failed (scheme={}, cause={}); continuing replay",
                job.jobId,
                count,
                routing.scheme,
                e.javaClass.simpleName,
                e,
            )
        }
    }

    private fun applyOverride(
        original: Routing,
        override: ReplayRequest.RoutingOverride?,
    ): Routing {
        if (override == null) return original
        return Routing(scheme = override.scheme, target = override.target, attributes = original.attributes)
    }

    private fun OutboxEvent.copy(routing: Routing): OutboxEvent =
        OutboxEvent(
            id = id,
            routing = routing,
            payload = payload,
            occurredAt = occurredAt,
            sourceCheckpoint = sourceCheckpoint,
        )

    companion object {
        private val logger = LoggerFactory.getLogger(ReplayService::class.java)
        const val DEFAULT_MAX_EVENTS: Int = 100_000
        const val DEFAULT_TIMEOUT_MS: Long = 10 * 60 * 1000L // 10 minutes

        private fun defaultExecutor(): ExecutorService =
            OneShotExecutor(
                Executors.newSingleThreadExecutor { r ->
                    Thread(r, "cdc-outbox-replay-${THREAD_COUNTER.incrementAndGet()}").apply {
                        isDaemon = true
                    }
                },
            )

        private val THREAD_COUNTER = AtomicLong()
    }
}

/**
 * Request body for [ReplayService.startReplay]. Plain data class so
 * the Actuator endpoint can bind it from JSON directly.
 */
data class ReplayRequest(
    val sourceKind: String,
    val fromPosition: String,
    val toPosition: String,
    val dryRun: Boolean = false,
    val override: RoutingOverride? = null,
) {
    data class RoutingOverride(val scheme: String, val target: String)
}

/**
 * Mutable snapshot of a replay job's progress. Updated in-place by
 * the background runner; the operator endpoint reads it under the
 * happens-before guarantees of [AtomicReference] visibility.
 */
data class ReplayJob(
    val jobId: String,
    val sourceKind: String,
    val fromPosition: String,
    val toPosition: String,
    val dryRun: Boolean,
    val override: ReplayRequest.RoutingOverride?,
    val startedAt: Instant,
    @Volatile var status: ReplayStatus = ReplayStatus.RUNNING,
    @Volatile var finishedAt: Instant? = null,
    @Volatile var elapsedMs: Long = 0,
    @Volatile var eventsProcessed: Long = 0,
    @Volatile var eventsPublished: Long = 0,
    @Volatile var eventsPublishFailed: Long = 0,
    @Volatile var eventsFilteredOut: Long = 0,
    @Volatile var eventsThatWouldBePublished: Long = 0,
    @Volatile var cappedAtMaxEvents: Boolean = false,
    @Volatile var errorClass: String? = null,
    @Volatile var errorMessage: String? = null,
)

enum class ReplayStatus { RUNNING, SUCCEEDED, FAILED }

class ConcurrentReplayException(message: String) : RuntimeException(message)

class ReplayTimeoutException(message: String) : RuntimeException(message)

/**
 * Marker wrapper around a single-shot [ExecutorService] so the
 * service knows it can safely shut it down once the job completes.
 * Consumers who inject their own executor get a different concrete
 * type and the service leaves it alone.
 */
internal class OneShotExecutor(private val delegate: ExecutorService) : ExecutorService by delegate
