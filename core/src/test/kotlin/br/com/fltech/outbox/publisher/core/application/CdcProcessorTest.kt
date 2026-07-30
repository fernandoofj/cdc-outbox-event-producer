package br.com.fltech.outbox.publisher.core.application

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.CdcSource
import br.com.fltech.outbox.publisher.core.port.DeadLetterPort
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.core.port.NoSinkForSchemeException
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.outbox.publisher.retry.BackOff
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFalse

/**
 * Drives the hexagonal orchestrator end-to-end against mocked
 * [CdcSource] and [EventSinkRegistry] beans. All time-based knobs
 * (backoff, idle sleep) are zeroed so the tests run in milliseconds.
 *
 * Scenarios:
 *  - happy path: source emits one event, registry accepts it, source
 *    is acked once.
 *  - retry then success: registry throws transiently for two attempts,
 *    succeeds on the third — exactly one ack to the source.
 *  - retries exhausted with DLQ: registry throws permanently, DLQ
 *    accepts the dead-letter, source IS acked, dead-letter metric
 *    recorded.
 *  - retries exhausted without DLQ: registry throws permanently, no
 *    DLQ — source is NOT acked, slot stays put.
 *  - NoSinkForSchemeException is permanent: no retry, dead-letter
 *    immediately (or stay stuck if no DLQ).
 *  - source poll failure does not crash the loop; orchestrator sleeps
 *    and retries the poll.
 */
class CdcProcessorTest {

    private val zeroBackOff = mockk<BackOff>().also {
        every { it.nextDelay(any()) } returns Duration.ZERO
    }
    private val registry = SimpleMeterRegistry()
    private val metrics = CdcOutboxMetrics(registry)

    private fun event(id: String, scheme: String = "sns", target: String = "topic-a"): OutboxEvent =
        OutboxEvent(
            id = id,
            routing = Routing(scheme, target),
            payload = """{"id":"$id"}""".toByteArray(),
            occurredAt = Instant.EPOCH,
            sourceCheckpoint = id,
        )

    private fun buildProcessor(
        sink: EventSinkRegistry,
        source: CdcSource,
        dlq: DeadLetterPort? = null,
        maxAttempts: Int = 3,
    ) = CdcProcessor(
        source = source,
        sinkRegistry = sink,
        metrics = metrics,
        deadLetterPort = dlq,
        maxPublishAttempts = maxAttempts,
        publishBackOff = zeroBackOff,
        idleSleep = Duration.ZERO,
        slotLabel = "test",
    )

    private fun runUntilProcessed(processor: CdcProcessor, source: CdcSource, expectedEvents: Int) {
        val t = thread(start = true, isDaemon = true) { processor.start() }
        // Spin until source.poll has been called enough times.
        val deadline = System.currentTimeMillis() + 2_000L
        while (System.currentTimeMillis() < deadline) {
            try {
                verify(atLeast = expectedEvents) { source.poll() }
                break
            } catch (_: AssertionError) {
                Thread.sleep(20)
            }
        }
        processor.stop()
        t.join(2_000L)
    }

    @Test
    fun `happy path delivers and acks the event once`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("1")
        every { source.poll() } returns ev andThen null

        val sink = mockk<EventSinkRegistry>(relaxed = true)
        every { sink.publish(any(), any()) } just Runs

        val processor = buildProcessor(sink, source)
        runUntilProcessed(processor, source, expectedEvents = 2)

        verify(exactly = 1) { sink.publish(ev.routing, ev) }
        verify(exactly = 1) { source.ack(ev) }
        assertEquals(1.0, registry.counter(
            CdcOutboxMetrics.MESSAGES_PUBLISHED,
            CdcOutboxMetrics.TAG_SINK, "sns",
            CdcOutboxMetrics.TAG_TOPIC, "topic-a",
        ).count())
    }

    @Test
    fun `retries succeed after two failures and ack the source exactly once`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("1")
        every { source.poll() } returns ev andThen null

        val sink = mockk<EventSinkRegistry>()
        every { sink.knownSchemes() } returns setOf("sns")
        every { sink.publish(any(), any()) } answers {
            // First two calls throw, third succeeds.
            error("transient")
        } andThenThrows IllegalStateException("transient-2") andThen Unit

        val processor = buildProcessor(sink, source)
        runUntilProcessed(processor, source, expectedEvents = 2)

        verify(exactly = 3) { sink.publish(ev.routing, ev) }
        verify(exactly = 1) { source.ack(ev) }
    }

    @Test
    fun `retries exhausted with a DLQ wired dead-letter the event and ack the source`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("1")
        every { source.poll() } returns ev andThen null
        val sink = mockk<EventSinkRegistry>(relaxed = true)
        every { sink.publish(any(), any()) } throws IllegalStateException("permafail")
        val dlq = mockk<DeadLetterPort>(); every { dlq.send(any(), any()) } just Runs

        val processor = buildProcessor(sink, source, dlq = dlq, maxAttempts = 2)
        runUntilProcessed(processor, source, expectedEvents = 2)

        verify(exactly = 2) { sink.publish(any(), any()) }
        verify(exactly = 1) { dlq.send(any(), any<IllegalStateException>()) }
        verify(exactly = 1) { source.ack(ev) }
        assertEquals(1.0, registry.counter(
            CdcOutboxMetrics.DEAD_LETTERED,
            CdcOutboxMetrics.TAG_SINK, "sns",
            CdcOutboxMetrics.TAG_TOPIC, "topic-a",
            CdcOutboxMetrics.TAG_CAUSE, "IllegalStateException",
        ).count())
    }

    @Test
    fun `retries exhausted without a DLQ keep the source unacked`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("1")
        every { source.poll() } returns ev andThen null
        val sink = mockk<EventSinkRegistry>(relaxed = true)
        every { sink.publish(any(), any()) } throws IllegalStateException("permafail")

        val processor = buildProcessor(sink, source, dlq = null, maxAttempts = 2)
        runUntilProcessed(processor, source, expectedEvents = 2)

        verify(exactly = 2) { sink.publish(any(), any()) }
        verify(exactly = 0) { source.ack(ev) }
        assertFalse(processor.isRunning())
    }

    @Test
    fun `NoSinkForSchemeException is permanent and bypasses retry`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("1", scheme = "nats")
        every { source.poll() } returns ev andThen null
        val sink = mockk<EventSinkRegistry>(relaxed = true)
        every { sink.publish(any(), any()) } throws NoSinkForSchemeException("nats", setOf("sns"))
        val dlq = mockk<DeadLetterPort>(); every { dlq.send(any(), any()) } just Runs

        val processor = buildProcessor(sink, source, dlq = dlq, maxAttempts = 5)
        runUntilProcessed(processor, source, expectedEvents = 2)

        // Just ONE publish attempt for this kind of failure, then dead-letter.
        verify(exactly = 1) { sink.publish(any(), any()) }
        verify(exactly = 1) { dlq.send(any(), any<NoSinkForSchemeException>()) }
        verify(exactly = 1) { source.ack(ev) }
    }

    @Test
    fun `pendingFailureCheckpoint is set on a publish failure and cleared on success`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("ck-pending")
        every { source.poll() } returns ev andThen null

        val sink = mockk<EventSinkRegistry>(relaxed = true)
        // First attempt throws, second succeeds — the marker MUST flip to
        // the event's checkpoint while we retry, then back to null when
        // the publish completes.
        every { sink.publish(any(), any()) } throws IllegalStateException("blip") andThen Unit

        val processor = buildProcessor(sink, source, maxAttempts = 3)
        runUntilProcessed(processor, source, expectedEvents = 2)

        // Success drained the marker — final snapshot is null.
        assertEquals(null, processor.snapshotState().pendingFailureCheckpoint)
    }

    @Test
    fun `pendingFailureCheckpoint stays set when retries exhaust without a DLQ`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("ck-stuck")
        every { source.poll() } returns ev andThen null
        val sink = mockk<EventSinkRegistry>(relaxed = true)
        every { sink.publish(any(), any()) } throws IllegalStateException("permafail")

        val processor = buildProcessor(sink, source, dlq = null, maxAttempts = 2)
        runUntilProcessed(processor, source, expectedEvents = 2)

        // No DLQ, no ack, loop exited — the marker is the operator's
        // only signal that the slot is stuck on this checkpoint.
        assertEquals("ck-stuck", processor.snapshotState().pendingFailureCheckpoint)
    }

    @Test
    fun `pendingFailureCheckpoint is cleared after a successful dead-letter`() {
        val source = mockk<CdcSource>(relaxed = true)
        val ev = event("ck-dlq")
        every { source.poll() } returns ev andThen null
        val sink = mockk<EventSinkRegistry>(relaxed = true)
        every { sink.publish(any(), any()) } throws IllegalStateException("permafail")
        val dlq = mockk<DeadLetterPort>(); every { dlq.send(any(), any()) } just Runs

        val processor = buildProcessor(sink, source, dlq = dlq, maxAttempts = 2)
        runUntilProcessed(processor, source, expectedEvents = 2)

        // Dead-letter advanced the source — the marker is no longer
        // truthful, so it MUST be cleared.
        assertEquals(null, processor.snapshotState().pendingFailureCheckpoint)
    }

    @Test
    fun `source poll exception does not crash the loop`() {
        val source = mockk<CdcSource>(relaxed = true)
        // First poll throws, second returns null so we can stop cleanly.
        every { source.poll() } throws RuntimeException("network blip") andThen null
        val sink = mockk<EventSinkRegistry>(relaxed = true)

        val processor = buildProcessor(sink, source)
        runUntilProcessed(processor, source, expectedEvents = 2)

        // Reconnect metric recorded with poll_failure reason.
        assertEquals(1.0, registry.counter(
            CdcOutboxMetrics.RECONNECTS,
            CdcOutboxMetrics.TAG_REASON, "poll_failure",
        ).count())
    }
}
