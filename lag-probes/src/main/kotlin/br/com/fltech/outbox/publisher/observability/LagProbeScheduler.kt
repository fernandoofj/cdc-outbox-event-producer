package br.com.fltech.outbox.publisher.observability

import br.com.fltech.outbox.publisher.core.port.LagProbe
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Periodically samples a [LagProbe] and feeds the value into a
 * Micrometer gauge.
 *
 * Decoupling sample from scrape: Micrometer gauges call their
 * supplier on every scrape. If the supplier hit the database
 * directly we'd add an unbounded per-scrape SQL call (Prometheus
 * scrapes every 5–15s typically, Datadog more aggressively, plus any
 * `/actuator/prometheus` probes). This scheduler maintains an
 * [AtomicLong] cache and registers a gauge that reads from it, so
 * the SQL cost is bounded by [intervalSeconds] regardless of how
 * many monitoring systems scrape the endpoint.
 *
 * The cache uses a sentinel ([UNSET]) for "no measurement yet". The
 * gauge supplier maps it to `Double.NaN`, which Micrometer emits as
 * "missing" on Prometheus and as `NaN` elsewhere — same shape the
 * probe contract specifies for `null` returns.
 *
 * Uses a [ScheduledExecutorService] with a daemon thread rather than
 * `@Scheduled`: the project deliberately avoids
 * `EnableScheduling` to keep auto-configuration's footprint small
 * and to keep this scheduler testable without a full Spring context.
 */
class LagProbeScheduler(
    private val probe: LagProbe,
    private val metrics: CdcOutboxMetrics,
    private val interval: Duration,
    /** Factory hook so tests can substitute a deterministic executor. */
    private val executorFactory: () -> ScheduledExecutorService = ::defaultExecutor,
) {
    private val cached = AtomicLong(UNSET)
    private val started = AtomicBoolean(false)

    @Volatile
    private var executor: ScheduledExecutorService? = null

    /**
     * Registers the gauge and starts the sampling loop. Idempotent —
     * calling twice is a no-op so Spring's `@PostConstruct` /
     * lifecycle wiring stays forgiving.
     */
    fun start() {
        if (!started.compareAndSet(false, true)) {
            logger.debug("LagProbeScheduler already started; ignoring duplicate start()")
            return
        }
        metrics.registerLagGauge(probe.sourceLabel) {
            val value = cached.get()
            if (value == UNSET) Double.NaN else value.toDouble()
        }
        val pool = executorFactory()
        executor = pool
        val intervalSeconds = interval.toSeconds().coerceAtLeast(1L)
        // Initial delay equals the interval — gives the source a beat
        // to settle before the first sample lands.
        pool.scheduleAtFixedRate(
            ::sample,
            intervalSeconds,
            intervalSeconds,
            TimeUnit.SECONDS,
        )
        logger.info(
            "LagProbeScheduler started for source='{}' interval={}s",
            probe.sourceLabel,
            intervalSeconds,
        )
    }

    /**
     * Stops the sampling loop. Idempotent; safe to call from Spring
     * `destroyMethod` or via [AutoCloseable]-style wiring.
     */
    fun stop() {
        if (!started.compareAndSet(true, false)) return
        executor?.let { pool ->
            pool.shutdown()
            val terminated =
                try {
                    pool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    logger.warn("Interrupted while awaiting LagProbeScheduler shutdown", e)
                    false
                }
            if (!terminated) {
                logger.warn(
                    "LagProbeScheduler did not terminate within {}s; sending shutdownNow.",
                    SHUTDOWN_TIMEOUT_SECONDS,
                )
                pool.shutdownNow()
            }
        }
        executor = null
        logger.info("LagProbeScheduler stopped for source='{}'", probe.sourceLabel)
    }

    /**
     * Performs one probe sample and updates the cached value.
     *
     * Visibility relaxed to `internal` so tests can invoke a single
     * tick without driving the executor. Any Exception is caught and
     * logged — letting an exception escape would cancel the scheduled
     * task silently (cf. `ScheduledExecutorService.scheduleAtFixedRate`
     * Javadoc), exactly the kind of silent failure
     * `feedback no_silent_errors` forbids. Errors (OOM, etc) are NOT
     * caught — they bubble to the JVM uncaught-exception handler, by
     * design.
     */
    internal fun sample() {
        try {
            val value = probe.lagBytes()
            if (value == null) {
                // Probe reported "no data" — leave the cached value
                // alone so the gauge keeps reading the last known
                // sample. Probes already log at WARN, so we stay
                // silent here.
                return
            }
            cached.set(value)
        } catch (e: Exception) {
            logger.warn(
                "LagProbeScheduler: probe '{}' threw ({}); keeping previous cached lag value.",
                probe.sourceLabel,
                e.javaClass.simpleName,
                e,
            )
        }
    }

    /** Exposes the cached sample for tests and diagnostics. */
    internal fun currentSample(): Long? {
        val value = cached.get()
        return if (value == UNSET) null else value
    }

    companion object {
        private val logger = LoggerFactory.getLogger(LagProbeScheduler::class.java)
        private const val SHUTDOWN_TIMEOUT_SECONDS = 5L
        private const val THREAD_NAME = "cdc-outbox-lag-probe"

        /**
         * Sentinel for "no sample yet". Picked deliberately as a
         * value the upstream cannot produce: lag is always
         * non-negative and bounded by `Long.MAX_VALUE`, while
         * `Long.MIN_VALUE` falls outside the legal range.
         */
        internal const val UNSET = Long.MIN_VALUE

        private fun defaultExecutor(): ScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor { r ->
                Thread(r, THREAD_NAME).apply { isDaemon = true }
            }
    }
}
