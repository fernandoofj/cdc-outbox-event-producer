package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CdcOutboxLifecycleTest {

    @Test
    fun `start launches the streaming loop on a daemon thread and stop signals it`() {
        val producer = mockk<SlotReaderMessageProducer>()
        val started = CountDownLatch(1)
        var observedDaemon = false
        var observedThreadName = ""

        every { producer.startStreaming() } answers {
            observedDaemon = Thread.currentThread().isDaemon
            observedThreadName = Thread.currentThread().name
            started.countDown()
            // Block until stopStreaming is observed, simulating the real loop.
            while (!Thread.currentThread().isInterrupted) {
                try {
                    Thread.sleep(SLEEP_INTERVAL_MS)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
            }
        }
        every { producer.stopStreaming() } just Runs

        val lifecycle = CdcOutboxLifecycle(producer)

        lifecycle.start()

        assertTrue(started.await(START_TIMEOUT_SECONDS, TimeUnit.SECONDS), "startStreaming was not invoked")
        assertTrue(lifecycle.isRunning)
        assertTrue(observedDaemon, "worker thread must be a daemon")
        assertTrue(observedThreadName.startsWith("cdc-outbox-slot-reader"))

        lifecycle.stop()
        verify(exactly = 1) { producer.stopStreaming() }
        assertFalse(lifecycle.isRunning)
    }

    @Test
    fun `start is idempotent`() {
        val producer = mockk<SlotReaderMessageProducer>()
        val started = CountDownLatch(1)
        every { producer.startStreaming() } answers {
            started.countDown()
            while (!Thread.currentThread().isInterrupted) {
                try { Thread.sleep(SLEEP_INTERVAL_MS) } catch (e: InterruptedException) { Thread.currentThread().interrupt() }
            }
        }
        every { producer.stopStreaming() } just Runs

        val lifecycle = CdcOutboxLifecycle(producer)

        lifecycle.start()
        started.await(START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        lifecycle.start() // second call should be a no-op

        verify(exactly = 1) { producer.startStreaming() }

        lifecycle.stop()
    }

    @Test
    fun `lifecycle phase positions the producer below the web server pair so HTTP is up first`() {
        val lifecycle = CdcOutboxLifecycle(mockk())
        assertEquals(CdcOutboxLifecycle.LIFECYCLE_PHASE, lifecycle.phase)
        // Below `WebServerStartStopLifecycle` (MAX_VALUE - 1) and
        // `WebServerGracefulShutdownLifecycle` (MAX_VALUE - 2048).
        assertTrue(lifecycle.phase < Int.MAX_VALUE - 2048)
        assertTrue(lifecycle.isAutoStartup)
    }

    companion object {
        private const val START_TIMEOUT_SECONDS = 2L
        private const val SLEEP_INTERVAL_MS = 50L
    }
}
