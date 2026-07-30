package br.com.fltech.outbox.publisher.adapter.dlq.replay

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DlqReplayServiceTest {

    private val reader = mockk<DlqReader>(relaxed = true)
    private val registry = mockk<EventSinkRegistry>(relaxed = true)
    private val meterRegistry = SimpleMeterRegistry()
    private val metrics = CdcOutboxMetrics(meterRegistry)
    private val service = DlqReplayService(reader, registry, metrics)

    @Test
    fun `peek delegates to reader and clamps the max within 1 and 10`() {
        every { reader.peek(any()) } returns emptyList()

        service.peek(max = 1000)

        verify { reader.peek(10) }
    }

    @Test
    fun `replay publishes through registry and deletes from DLQ on success`() {
        val envelope = sampleEnvelope("sns://orders-events")
        val capturedRouting = slot<Routing>()
        val capturedEvent = slot<OutboxEvent>()
        every { registry.publish(capture(capturedRouting), capture(capturedEvent)) } returns Unit

        val outcome = service.replay("AQEB-handle", envelope)

        assertTrue(outcome is DlqReplayService.ReplayOutcome.Success)
        assertEquals("sns", capturedRouting.captured.scheme)
        assertEquals("orders-events", capturedRouting.captured.target)
        assertEquals(envelope.lsn, capturedEvent.captured.id)
        assertEquals(envelope.lsn, capturedEvent.captured.sourceCheckpoint)
        verify { reader.delete(DlqReader.Handle("AQEB-handle")) }

        val counter = meterRegistry.find(CdcOutboxMetrics.DLQ_REPLAYS)
            .tag(CdcOutboxMetrics.TAG_OUTCOME, "success")
            .counter()
        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `replay does NOT delete from DLQ when publish raises`() {
        val envelope = sampleEnvelope("sns://orders-events")
        every { registry.publish(any(), any()) } throws IllegalStateException("broker down")

        val outcome = service.replay("AQEB-handle", envelope)

        assertTrue(outcome is DlqReplayService.ReplayOutcome.PublishFailed)
        verify(exactly = 0) { reader.delete(any()) }

        val counter = meterRegistry.find(CdcOutboxMetrics.DLQ_REPLAYS)
            .tag(CdcOutboxMetrics.TAG_OUTCOME, "publish_failed")
            .counter()
        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `replay surfaces success_delete_failed when publish ok but delete raises`() {
        val envelope = sampleEnvelope("sns://orders-events")
        every { registry.publish(any(), any()) } returns Unit
        every { reader.delete(any()) } throws IllegalStateException("expired handle")

        val outcome = service.replay("AQEB-handle", envelope)

        assertTrue(outcome is DlqReplayService.ReplayOutcome.SuccessButDeleteFailed)

        val counter = meterRegistry.find(CdcOutboxMetrics.DLQ_REPLAYS)
            .tag(CdcOutboxMetrics.TAG_OUTCOME, "success_delete_failed")
            .counter()
        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `replay honours operator override scheme and target`() {
        val envelope = sampleEnvelope(prefix = "sns://orders-events")
        val capturedRouting = slot<Routing>()
        every { registry.publish(capture(capturedRouting), any()) } returns Unit

        service.replay(
            "h",
            envelope,
            DlqReplayService.RoutingOverride(scheme = "kafka", target = "orders.topic"),
        )

        assertEquals("kafka", capturedRouting.captured.scheme)
        assertEquals("orders.topic", capturedRouting.captured.target)
    }

    @Test
    fun `bulk dry-run does NOT publish nor delete anything`() {
        every { reader.peek(any()) } returns
            listOf(
                DlqReader.Message(DlqReader.Handle("h1"), sampleEnvelope("sns://orders-events")),
                DlqReader.Message(DlqReader.Handle("h2"), sampleEnvelope("sns://billing-events")),
            )

        val result = service.replayBulk(max = 5, dryRun = true)

        assertTrue(result.dryRun)
        assertEquals(2, result.attempted)
        assertEquals(2, result.previews.size)
        assertEquals(0, result.succeeded)
        verify(exactly = 0) { registry.publish(any(), any()) }
        verify(exactly = 0) { reader.delete(any()) }
    }

    @Test
    fun `bulk live mode publishes each peeked message and counts outcomes`() {
        every { reader.peek(any()) } returns
            listOf(
                DlqReader.Message(DlqReader.Handle("h1"), sampleEnvelope("sns://orders-events")),
                DlqReader.Message(DlqReader.Handle("h2"), sampleEnvelope("sns://billing-events")),
                DlqReader.Message(DlqReader.Handle("h3"), sampleEnvelope("sns://shipping-events")),
            )
        // Second publish fails; first and third succeed.
        every { registry.publish(any(), any()) } returnsMany
            listOf(Unit) andThenThrows IllegalStateException("broker glitch") andThenAnswer { Unit }

        val result = service.replayBulk(max = 10, dryRun = false)

        assertEquals(3, result.attempted)
        assertEquals(2, result.succeeded)
        assertEquals(1, result.failed)
        assertFalse(result.dryRun)
    }

    @Test
    fun `abandon deletes via reader and records the metric`() {
        val outcome = service.abandon("AQEB-handle")

        assertTrue(outcome is DlqReplayService.AbandonOutcome.Success)
        verify { reader.delete(DlqReader.Handle("AQEB-handle")) }

        val counter = meterRegistry.find(CdcOutboxMetrics.DLQ_REPLAYS)
            .tag(CdcOutboxMetrics.TAG_OUTCOME, "abandoned")
            .counter()
        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `abandon reports failure when reader raises`() {
        every { reader.delete(any()) } throws IllegalStateException("expired")

        val outcome = service.abandon("AQEB-handle")

        assertTrue(outcome is DlqReplayService.AbandonOutcome.Failed)
        val counter = meterRegistry.find(CdcOutboxMetrics.DLQ_REPLAYS)
            .tag(CdcOutboxMetrics.TAG_OUTCOME, "abandon_failed")
            .counter()
        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `replay falls back to current time when deadLetteredAt does not parse`() {
        val malformedDate = DlqEnvelope(
            originalPrefix = "sns://orders-events",
            lsn = "0/16E8198",
            content = "{}",
            failureType = "TimeoutException",
            failureMessage = "",
            deadLetteredAt = "not-a-valid-instant",
        )
        val capturedEvent = slot<OutboxEvent>()
        every { registry.publish(any(), capture(capturedEvent)) } returns Unit

        service.replay("h", malformedDate)

        // Used now() rather than aborting — diagnostic field, not load-bearing.
        assertTrue(capturedEvent.captured.occurredAt.epochSecond > 0)
    }

    private fun sampleEnvelope(prefix: String) = DlqEnvelope(
        originalPrefix = prefix,
        lsn = "0/16E8198",
        content = """{"orderId":42}""",
        failureType = "TimeoutException",
        failureMessage = "publish timed out",
        deadLetteredAt = "2026-05-15T10:00:00Z",
    )
}
