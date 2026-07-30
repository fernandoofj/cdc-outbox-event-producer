package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.health.Status
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CdcOutboxHealthIndicatorTest {

    private val producer = mockk<SlotReaderMessageProducer>()
    private val lifecycle = mockk<CdcOutboxLifecycle>()
    private val indicator = CdcOutboxHealthIndicator(producer, lifecycle, Duration.ofMinutes(10))

    @Test
    fun `UP when producer is running, no pending failure, and activity within maxIdle`() {
        every { producer.snapshotState() } returns SlotReaderMessageProducer.ProducerState(
            slot = "orders_outbox_slot",
            running = true,
            pendingFailureLsn = null,
            msSinceLastActivity = 1_000L,
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.UP, health.status)
        assertEquals("orders_outbox_slot", health.details["slot"])
        assertEquals(true, health.details["running"])
        assertEquals("none", health.details["pendingFailureLsn"])
        assertEquals("1000ms", health.details["idleFor"])
    }

    @Test
    fun `DOWN when a publish failure is pending, regardless of running status`() {
        every { producer.snapshotState() } returns SlotReaderMessageProducer.ProducerState(
            slot = "orders_outbox_slot",
            running = true,
            pendingFailureLsn = "0/16E8198",
            msSinceLastActivity = 100L,
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.DOWN, health.status)
        assertEquals("0/16E8198", health.details["pendingFailureLsn"])
        assertTrue((health.details["reason"] as String).contains("pending publish failure"))
    }

    @Test
    fun `DOWN when the producer is not running`() {
        every { producer.snapshotState() } returns SlotReaderMessageProducer.ProducerState(
            slot = "orders_outbox_slot",
            running = false,
            pendingFailureLsn = null,
            msSinceLastActivity = 100L,
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.DOWN, health.status)
        assertEquals(false, health.details["running"])
        assertEquals(true, health.details["lifecycleRunning"])
        assertEquals("producer is not running", health.details["reason"])
    }

    @Test
    fun `OUT_OF_SERVICE when idle beyond maxIdle`() {
        every { producer.snapshotState() } returns SlotReaderMessageProducer.ProducerState(
            slot = "orders_outbox_slot",
            running = true,
            pendingFailureLsn = null,
            msSinceLastActivity = Duration.ofMinutes(20).toMillis(),
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        assertEquals(Status.OUT_OF_SERVICE, health.status)
        assertTrue((health.details["reason"] as String).contains("no activity"))
    }

    @Test
    fun `Long MAX VALUE idle (loop never started yet) is treated as not-idle and renders as 'never'`() {
        every { producer.snapshotState() } returns SlotReaderMessageProducer.ProducerState(
            slot = "orders_outbox_slot",
            running = true,
            pendingFailureLsn = null,
            msSinceLastActivity = Long.MAX_VALUE,
        )
        every { lifecycle.isRunning } returns true

        val health = indicator.health()

        // running = true wins; OUT_OF_SERVICE is only triggered by finite idle > threshold.
        assertEquals(Status.UP, health.status)
        assertEquals("never", health.details["idleFor"])
    }
}
