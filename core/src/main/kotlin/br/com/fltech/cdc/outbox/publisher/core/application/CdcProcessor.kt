package br.com.fltech.cdc.outbox.publisher.core.application

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource
import br.com.fltech.cdc.outbox.publisher.core.port.DeadLetterPort
import br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.cdc.outbox.publisher.core.port.NoSinkForSchemeException
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.cdc.outbox.publisher.retry.BackOff
import br.com.fltech.cdc.outbox.publisher.retry.ExponentialBackOff
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Hexagonal-architecture orchestrator: pulls [OutboxEvent]s from a
 * [CdcSource], hands each event to the [EventSinkRegistry] for
 * publication, and `ack`s the source only after the publish (or
 * dead-letter) terminates successfully.
 *
 * The retry / dead-letter state machine mirrors what
 * `SlotReaderMessageProducer` already implements, but here the
 * publish step goes through `registry.publish(routing, event)` instead
 * of a hard-coded `when (DestinationType)` switch — adding a new
 * broker becomes "register an `EventSink` under a new scheme" rather
 * than editing this class.
 *
 * Invariants:
 *  - Source is `ack`ed exactly once per event, AFTER the publish or
 *    dead-letter succeeded. If the publish fails permanently and no
 *    DLQ is configured, the source is NOT acked — the source is then
 *    responsible for redelivery on the next iteration / restart, which
 *    preserves at-least-once.
 *  - Configuration / scheme errors (`NoSinkForSchemeException`) are
 *    treated as permanent: the orchestrator does not retry them. If no
 *    DLQ is configured the source stays stuck — operator must
 *    intervene. With a DLQ the bad event is dead-lettered and the
 *    source advances.
 *  - `running` is `@Volatile` so `stop()` from another thread is
 *    observed promptly.
 */
@Suppress("LongParameterList")
class CdcProcessor(
    private val source: CdcSource,
    private val sinkRegistry: EventSinkRegistry,
    private val metrics: CdcOutboxMetrics = CdcOutboxMetrics.noop(),
    private val deadLetterPort: DeadLetterPort? = null,
    private val maxPublishAttempts: Int = DEFAULT_MAX_PUBLISH_ATTEMPTS,
    private val publishBackOff: BackOff = ExponentialBackOff(
        initial = Duration.ofMillis(DEFAULT_BACKOFF_INITIAL_MS),
        max = Duration.ofSeconds(DEFAULT_BACKOFF_MAX_SECONDS),
    ),
    private val idleSleep: Duration = Duration.ofMillis(DEFAULT_IDLE_SLEEP_MS),
    private val slotLabel: String = "cdc-processor",
) {

    @Volatile
    private var running = false

    /**
     * Epoch-millis timestamp of the last `source.poll()` call. `0L`
     * means "the loop has never iterated" — translated to
     * [Long.MAX_VALUE] in `snapshotState().msSinceLastActivity` to
     * match the convention of `SlotReaderMessageProducer.snapshotState`
     * (so health indicators can treat "never" as "not idle for
     * threshold purposes"). `@Volatile` because the health indicator
     * reads it from a different thread.
     */
    @Volatile
    private var lastActivityMs: Long = 0L

    /**
     * Checkpoint of the event currently stuck in retry. Set when a
     * publish raises and we're about to back off, cleared when the
     * event eventually succeeds OR gets dead-lettered (the source IS
     * acked on dead-letter, so the slot moves on — `null` is the
     * correct steady state). The health indicator surfaces this as
     * the hex equivalent of the legacy `pendingFailureLsn`.
     * `@Volatile` because the indicator reads from a different
     * thread.
     */
    @Volatile
    private var pendingFailureCheckpoint: String? = null

    fun start() {
        if (running) {
            logger.debug("CdcProcessor already running; ignoring duplicate start()")
            return
        }
        source.open()
        running = true
        logger.info("CdcProcessor started ({})", slotLabel)
        try {
            runLoop()
        } finally {
            runCatching { source.close() }
            running = false
            logger.info("CdcProcessor stopped ({})", slotLabel)
        }
    }

    fun stop() {
        running = false
    }

    fun isRunning(): Boolean = running

    /**
     * Read-only snapshot of orchestrator state for health checks. Safe
     * to call from a different thread (relies on `@Volatile` fields).
     */
    fun snapshotState(): ProcessorState = ProcessorState(
        slot = slotLabel,
        running = running,
        msSinceLastActivity = msSinceLastActivity(),
        pendingFailureCheckpoint = pendingFailureCheckpoint,
    )

    private fun msSinceLastActivity(): Long {
        val ts = lastActivityMs
        return if (ts == 0L) Long.MAX_VALUE else System.currentTimeMillis() - ts
    }

    /**
     * Health-check projection of the orchestrator. `msSinceLastActivity`
     * uses [Long.MAX_VALUE] as the "never iterated" sentinel — same
     * convention as `SlotReaderMessageProducer.ProducerState` so the
     * two health indicators behave identically when the loop has not
     * yet started. `pendingFailureCheckpoint` mirrors the legacy
     * indicator's `pendingFailureLsn`: non-null means the loop is
     * stuck on a single event whose publish keeps failing and the
     * source has not been acked yet.
     */
    data class ProcessorState(
        val slot: String,
        val running: Boolean,
        val msSinceLastActivity: Long,
        val pendingFailureCheckpoint: String? = null,
    )

    private fun runLoop() {
        while (running) {
            // Record activity BEFORE the poll so a hang inside poll
            // counts as "active" until the call returns — the goal of
            // the idle metric is to detect a stalled loop, not a slow
            // upstream. The order also means the very first iteration
            // moves `lastActivityMs` off `0L`, flipping the "never
            // iterated" sentinel to a real elapsed measurement.
            lastActivityMs = System.currentTimeMillis()
            when (val result = pollOnce()) {
                is PollResult.Failure -> {
                    logger.error(
                        "CdcSource.poll() threw; sleeping {} ms before retrying",
                        idleSleep.toMillis(), result.cause,
                    )
                    metrics.recordReconnect(REASON_POLL_FAILURE)
                    sleepInterruptibly(idleSleep.toMillis())
                }
                is PollResult.Empty -> sleepInterruptibly(idleSleep.toMillis())
                is PollResult.Event -> {
                    metrics.recordMessageRead(slotLabel)
                    processEvent(result.event)
                }
            }
        }
    }

    private fun pollOnce(): PollResult =
        try {
            val event = source.poll()
            if (event == null) PollResult.Empty else PollResult.Event(event)
        } catch (e: Exception) {
            PollResult.Failure(e)
        }

    /** Tri-state result of a single source.poll() — drives runLoop's dispatch. */
    private sealed class PollResult {
        object Empty : PollResult()
        data class Event(val event: OutboxEvent) : PollResult()
        data class Failure(val cause: Exception) : PollResult()
    }

    // ReturnCount: 3 explicit returns map to 3 distinct outcomes
    // (successful publish, scheme misconfigured, shutdown-during-retry).
    // Folding them into a single return path loses the per-outcome
    // logging clarity right above each return.
    @Suppress("ReturnCount")
    private fun processEvent(event: OutboxEvent) {
        var attempt = 0
        var lastException: Exception? = null
        while (attempt < maxPublishAttempts && running) {
            attempt += 1
            try {
                sinkRegistry.publish(event.routing, event)
                metrics.recordPublished(sink = event.routing.scheme, topic = event.routing.target)
                source.ack(event)
                // Successful publish drains any pending-failure marker —
                // the slot moved past this event.
                pendingFailureCheckpoint = null
                return
            } catch (e: NoSinkForSchemeException) {
                // Configuration error — retries are pointless.
                logger.error(
                    "No sink for scheme '{}' (event id={}, target={}). Known: {}",
                    e.scheme, event.id, event.routing.target, e.knownSchemes,
                )
                metrics.recordFailure(
                    sink = e.scheme,
                    topic = event.routing.target,
                    cause = e.javaClass.simpleName,
                )
                handleExhausted(event, e)
                return
            } catch (e: Exception) {
                lastException = e
                // Surface the in-flight checkpoint to the health
                // indicator the moment the first attempt fails; we
                // clear it on success or dead-letter, so a transient
                // blip that resolves on retry briefly toggles
                // /actuator/health DOWN — same precedence the legacy
                // indicator already exposes.
                pendingFailureCheckpoint = event.sourceCheckpoint
                metrics.recordFailure(
                    sink = event.routing.scheme,
                    topic = event.routing.target,
                    cause = e.javaClass.simpleName,
                )
                if (attempt < maxPublishAttempts) {
                    metrics.recordRetry(
                        sink = event.routing.scheme,
                        topic = event.routing.target,
                        attempt = attempt,
                    )
                    val delay = publishBackOff.nextDelay(attempt)
                    logger.warn(
                        "Publish attempt #{}/{} for {} (id={}) failed: {}. Sleeping {} ms before retry.",
                        attempt, maxPublishAttempts, event.routing.asUri(), event.id,
                        e.javaClass.simpleName, delay.toMillis(),
                    )
                    sleepInterruptibly(delay.toMillis())
                }
            }
        }
        if (!running) {
            // Shutdown raced with a publish failure — do NOT dead-letter; leave
            // the source unacked so the next start replays the message. At-least-
            // once is preserved without prematurely persisting a "failed" record.
            // Leave `pendingFailureCheckpoint` set so the indicator reports
            // DOWN until the next start replays and either succeeds or
            // dead-letters; otherwise the operator would see UP on a stuck
            // slot.
            logger.info(
                "Shutdown requested mid-retry for event id={} routing={}; source will not be acked.",
                event.id, event.routing.asUri(),
            )
            return
        }
        val cause = lastException ?: IllegalStateException("publish retries exhausted without an exception")
        handleExhausted(event, cause)
    }

    private fun handleExhausted(event: OutboxEvent, cause: Throwable) {
        val dlq = deadLetterPort
        if (dlq == null) {
            logger.error(
                "Publish exhausted {} attempts for {} (id={}) and no dead-letter port is configured — " +
                    "source will not be acked. Operator intervention required.",
                maxPublishAttempts, event.routing.asUri(), event.id, cause,
            )
            return
        }
        try {
            dlq.send(event, cause)
            metrics.recordDeadLettered(
                sink = event.routing.scheme,
                topic = event.routing.target,
                cause = cause.javaClass.simpleName,
            )
            source.ack(event)
            // The slot advanced via dead-letter — pending-failure marker
            // is no longer accurate.
            pendingFailureCheckpoint = null
            logger.warn(
                "Dead-lettered event id={} routing={} after {} attempts ({})",
                event.id, event.routing.asUri(), maxPublishAttempts, cause.javaClass.simpleName,
            )
        } catch (dlqException: Exception) {
            metrics.recordDeadLetterFailure(cause = dlqException.javaClass.simpleName)
            logger.error(
                "Dead-letter publish failed for event id={} routing={}; source will not be acked. " +
                    "Original cause: {}; DLQ cause: {}",
                event.id, event.routing.asUri(),
                cause.javaClass.simpleName, dlqException.javaClass.simpleName, dlqException,
            )
        }
    }

    private fun sleepInterruptibly(millis: Long) {
        if (millis <= 0L || !running) return
        try {
            Thread.sleep(millis)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            running = false
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CdcProcessor::class.java)
        const val DEFAULT_MAX_PUBLISH_ATTEMPTS = 5
        const val DEFAULT_BACKOFF_INITIAL_MS = 100L
        const val DEFAULT_BACKOFF_MAX_SECONDS = 5L
        const val DEFAULT_IDLE_SLEEP_MS = 100L
        private const val REASON_POLL_FAILURE = "poll_failure"
    }
}
