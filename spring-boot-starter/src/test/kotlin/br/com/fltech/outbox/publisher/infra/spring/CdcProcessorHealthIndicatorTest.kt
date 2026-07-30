package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.core.application.CdcProcessor
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.health.Status
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Drives the hex orchestrator health indicator across the four
 * states the production decision matrix recognises:
 *  - UP — lifecycle on, processor on, idle within threshold.
 *  - DOWN — processor loop exited while the lifecycle still claims
 *    to be running (orchestrator crashed or shutdown race).
 *  - DOWN — lifecycle has not been started yet (Spring context
 *    refresh failed to drive `lifecycle.start()`).
 *  - OUT_OF_SERVICE — both are running but `msSinceLastActivity`
 *    exceeded the configured `maxIdle`.
 *
 * The "never iterated" sentinel ([Long.MAX_VALUE]) is exercised
 * implicitly under the UP case so the indicator is well-behaved
 * during the brief window between context refresh and the first
 * loop turn.
 */
class CdcProcessorHealthIndicatorTest {

    private val processor = mockk<CdcProcessor>()
    private val lifecycle = mockk<CdcProcessorLifecycle>()
    private val indicator = CdcProcessorHealthIndicator(processor, lifecycle, Duration.ofMinutes(10))

    @Test
    fun `UP when both lifecycle and processor are running and idle is within threshold`() {
        every { processor.snapshotState() } returns CdcProcessor.ProcessorState(
            slot = "orders_outbox_slot",
            running = true,
            msSinceLastActivity = 1_000L,
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.UP, health.status)
        assertEquals("orders_outbox_slot", health.details["slot"])
        assertEquals(true, health.details["processorRunning"])
        assertEquals(true, health.details["lifecycleRunning"])
        assertEquals("1000ms", health.details["idleFor"])
    }

    @Test
    fun `DOWN when the lifecycle is up but the processor loop is not iterating`() {
        every { processor.snapshotState() } returns CdcProcessor.ProcessorState(
            slot = "orders_outbox_slot",
            running = false,
            msSinceLastActivity = 100L,
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.DOWN, health.status)
        assertEquals(false, health.details["processorRunning"])
        assertEquals(true, health.details["lifecycleRunning"])
        assertTrue((health.details["reason"] as String).contains("processor loop is not iterating"))
    }

    @Test
    fun `DOWN when the lifecycle has not started — takes precedence over processor flag`() {
        // Even if the processor flag says true, the lifecycle gate
        // wins: Spring has not yet driven `start()`, so nothing is
        // streaming yet from the Actuator probe's perspective.
        every { processor.snapshotState() } returns CdcProcessor.ProcessorState(
            slot = "orders_outbox_slot",
            running = true,
            msSinceLastActivity = 100L,
        )
        every { lifecycle.isRunning } returns false

        val health = indicator.health()

        assertEquals(Status.DOWN, health.status)
        assertEquals(false, health.details["lifecycleRunning"])
        assertEquals("lifecycle has not started", health.details["reason"])
    }

    @Test
    fun `OUT_OF_SERVICE when idle exceeds maxIdle`() {
        every { processor.snapshotState() } returns CdcProcessor.ProcessorState(
            slot = "orders_outbox_slot",
            running = true,
            msSinceLastActivity = Duration.ofMinutes(20).toMillis(),
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.OUT_OF_SERVICE, health.status)
        assertTrue((health.details["reason"] as String).contains("no activity"))
    }

    @Test
    fun `Long MAX VALUE idle sentinel is treated as not-idle and renders as 'never'`() {
        // The grace window between context refresh and the first
        // `source.poll()` MUST NOT degrade /actuator/health — running
        // wins.
        every { processor.snapshotState() } returns CdcProcessor.ProcessorState(
            slot = "orders_outbox_slot",
            running = true,
            msSinceLastActivity = Long.MAX_VALUE,
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.UP, health.status)
        assertEquals("never", health.details["idleFor"])
    }

    @Test
    fun `DOWN when a pending publish failure is reported — takes precedence over everything`() {
        // Even if everything else looks healthy, a pending-failure
        // checkpoint means the source has NOT been acked past the
        // troubled event. That's an operator signal, not an idle
        // condition. Matches the legacy indicator's
        // pendingFailureLsn precedence.
        every { processor.snapshotState() } returns CdcProcessor.ProcessorState(
            slot = "orders_outbox_slot",
            running = true,
            msSinceLastActivity = 100L,
            pendingFailureCheckpoint = "mysql-bin.000001:240",
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.DOWN, health.status)
        assertEquals("mysql-bin.000001:240", health.details["pendingFailureCheckpoint"])
        assertTrue((health.details["reason"] as String).contains("pending publish failure"))
    }
}
