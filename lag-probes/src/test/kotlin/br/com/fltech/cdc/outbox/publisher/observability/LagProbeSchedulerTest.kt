package br.com.fltech.cdc.outbox.publisher.observability

import br.com.fltech.cdc.outbox.publisher.core.port.LagProbe
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Unit tests for [LagProbeScheduler]. Verifies:
 *  - registerLagGauge fires on start with the probe's sourceLabel,
 *  - the gauge reads from the cached sample,
 *  - missing samples (probe returns null) leave the cache alone,
 *  - thrown exceptions don't kill the scheduler thread,
 *  - start/stop are idempotent.
 *
 * Uses a single-thread executor that runs `sample()` directly when
 * scheduled so the tests stay deterministic without sleeping for
 * real-time intervals.
 */
class LagProbeSchedulerTest {
    private fun fixedExecutor(): ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "lag-probe-test").apply { isDaemon = true }
        }

    @Test
    fun `start registers a gauge under the probe sourceLabel`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val probe = StubProbe("postgres", values = listOf(7_777L))
        val scheduler =
            LagProbeScheduler(
                probe = probe,
                metrics = metrics,
                interval = Duration.ofSeconds(1),
                executorFactory = ::fixedExecutor,
            )

        scheduler.start()
        try {
            // Drive a single sample tick so the gauge has a real value.
            scheduler.sample()

            val gauge =
                registry.find(CdcOutboxMetrics.SOURCE_LAG_BYTES)
                    .tag(CdcOutboxMetrics.TAG_SOURCE, "postgres")
                    .gauge()
            assertNotNull(gauge)
            assertEquals(7_777.0, gauge.value())
        } finally {
            scheduler.stop()
        }
    }

    @Test
    fun `gauge reads NaN before the first sample lands`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val probe = StubProbe("mysql", values = emptyList())
        val scheduler =
            LagProbeScheduler(
                probe = probe,
                metrics = metrics,
                interval = Duration.ofSeconds(1),
                executorFactory = ::fixedExecutor,
            )

        scheduler.start()
        try {
            val gauge =
                registry.find(CdcOutboxMetrics.SOURCE_LAG_BYTES)
                    .tag(CdcOutboxMetrics.TAG_SOURCE, "mysql")
                    .gauge()
            assertNotNull(gauge)
            assertTrue(gauge.value().isNaN())
        } finally {
            scheduler.stop()
        }
    }

    @Test
    fun `null probe result leaves the previous cached value intact`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val probe = StubProbe("postgres", values = listOf(100L, null, 200L))
        val scheduler =
            LagProbeScheduler(
                probe = probe,
                metrics = metrics,
                interval = Duration.ofSeconds(1),
                executorFactory = ::fixedExecutor,
            )

        scheduler.start()
        try {
            scheduler.sample()
            assertEquals(100L, scheduler.currentSample())

            // Probe returns null this tick → cache unchanged.
            scheduler.sample()
            assertEquals(100L, scheduler.currentSample())

            // Then a fresh value lands.
            scheduler.sample()
            assertEquals(200L, scheduler.currentSample())
        } finally {
            scheduler.stop()
        }
    }

    @Test
    fun `exceptions thrown by the probe are swallowed without killing the loop`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val probe = ThrowingProbe("postgres")
        val scheduler =
            LagProbeScheduler(
                probe = probe,
                metrics = metrics,
                interval = Duration.ofSeconds(1),
                executorFactory = ::fixedExecutor,
            )

        scheduler.start()
        try {
            // Must not propagate the exception.
            scheduler.sample()
            assertNull(scheduler.currentSample())
        } finally {
            scheduler.stop()
        }
    }

    @Test
    fun `start is idempotent`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val probe = StubProbe("postgres", values = listOf(1L))
        val factoryCalls = AtomicInteger(0)
        val scheduler =
            LagProbeScheduler(
                probe = probe,
                metrics = metrics,
                interval = Duration.ofSeconds(1),
                executorFactory = {
                    factoryCalls.incrementAndGet()
                    fixedExecutor()
                },
            )

        scheduler.start()
        scheduler.start()
        try {
            assertEquals(1, factoryCalls.get())
        } finally {
            scheduler.stop()
        }
    }

    @Test
    fun `stop releases the executor and a second stop is a no-op`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val probe = StubProbe("postgres", values = emptyList())
        val executor = fixedExecutor()
        val scheduler =
            LagProbeScheduler(
                probe = probe,
                metrics = metrics,
                interval = Duration.ofSeconds(1),
                executorFactory = { executor },
            )

        scheduler.start()
        scheduler.stop()
        // Second stop is a no-op — must not throw.
        scheduler.stop()

        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS))
        assertTrue(executor.isShutdown)
    }

    @Test
    fun `interval below one second is clamped to one second`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val probe = StubProbe("postgres", values = listOf(42L))
        // 0ms would otherwise produce a 0-second period that
        // ScheduledExecutorService rejects as `IllegalArgumentException`.
        val scheduler =
            LagProbeScheduler(
                probe = probe,
                metrics = metrics,
                interval = Duration.ofMillis(0),
                executorFactory = ::fixedExecutor,
            )

        scheduler.start()
        try {
            // No assertion beyond "didn't throw" — clamp is internal,
            // verified by the executor accepting the schedule.
            scheduler.sample()
            assertEquals(42L, scheduler.currentSample())
        } finally {
            scheduler.stop()
        }
    }

    /** Fake probe that returns a fixed sequence of values, looping on the last entry. */
    private class StubProbe(
        override val sourceLabel: String,
        values: List<Long?>,
    ) : LagProbe {
        private val iter = values.iterator()
        private var last: Long? = null

        override fun lagBytes(): Long? {
            if (iter.hasNext()) last = iter.next()
            return last
        }
    }

    private class ThrowingProbe(override val sourceLabel: String) : LagProbe {
        override fun lagBytes(): Long? = error("boom")
    }
}
