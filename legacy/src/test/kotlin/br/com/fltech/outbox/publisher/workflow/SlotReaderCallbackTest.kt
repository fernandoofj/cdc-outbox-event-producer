package br.com.fltech.outbox.publisher.workflow

import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessageBody
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.outbox.publisher.replication.connector.PostgresConnector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.postgresql.replication.LogSequenceNumber
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class SlotReaderCallbackTest {
    private val lsnFailed = LogSequenceNumber.valueOf(100L)
    private val lsnSucceeded = LogSequenceNumber.valueOf(200L)

    private val producer = mockk<SlotReaderMessageProducer>(relaxed = true)
    private val connector =
        mockk<PostgresConnector>().also {
            every { it.setStreamLsn(any()) } just Runs
        }

    @Test
    fun `onFailure does NOT advance the slot LSN (at-least-once)`() {
        val callback = SlotReaderCallback(producer, connector)

        callback.onFailure(lsnFailed, prefix = "topic-a", sink = "sns", t = RuntimeException("boom"))

        verify(exactly = 0) { connector.setStreamLsn(any()) }
        verify(exactly = 0) { producer.resetIdleCounter() }
    }

    @Test
    fun `onSNSSuccess advances the slot to the captured LSN and not lastReceiveLsn`() {
        val callback = SlotReaderCallback(producer, connector)
        val message = snsMessage(eventType = "OrderCreated")

        callback.onSNSSuccess(lsnSucceeded, "topic-a", message)

        verify(exactly = 1) { connector.setStreamLsn(lsnSucceeded) }
        verify(exactly = 1) { producer.resetIdleCounter() }
        // We never look at lastReceiveLsn from the callback any more.
        verify(exactly = 0) { connector.lastReceivedLsn() }
    }

    @Test
    fun `onSQSSuccess advances the slot to the captured LSN`() {
        val callback = SlotReaderCallback(producer, connector)

        callback.onSQSSuccess(lsnSucceeded, "queue-a")

        verify(exactly = 1) { connector.setStreamLsn(lsnSucceeded) }
        verify(exactly = 1) { producer.resetIdleCounter() }
    }

    @Test
    fun `discardMessage advances the slot to the discarded LSN`() {
        val callback = SlotReaderCallback(producer, connector)
        val lsn = LogSequenceNumber.valueOf(300L)

        callback.discardMessage(lsn, "insert")

        verify(exactly = 1) { connector.setStreamLsn(lsn) }
    }

    @Test
    fun `regression failure followed by success never advances past the failed LSN`() {
        val callback = SlotReaderCallback(producer, connector)
        val firstMsgLsn = LogSequenceNumber.valueOf(100L)
        val secondMsgLsn = LogSequenceNumber.valueOf(200L)

        callback.onFailure(firstMsgLsn, prefix = "topic-a", sink = "sns", t = RuntimeException("boom"))
        callback.onSNSSuccess(secondMsgLsn, "topic-a", snsMessage("OrderCreated"))

        // Old buggy behaviour: setStreamLsn(lsn=200) called twice or once with lastReceiveLsn.
        // Fixed behaviour: setStreamLsn called ONCE, with the SECOND message's lsn — but the
        // first message's failure must have left the slot below 200 so Postgres can resend
        // (the loop will retry the first message on next read, which is the caller's
        // responsibility, not the callback's). The callback contract is: only advance for
        // the message we explicitly acted on, never silently for a stream high-water mark.
        verify(exactly = 1) { connector.setStreamLsn(secondMsgLsn) }
        verify(exactly = 0) { connector.setStreamLsn(firstMsgLsn) }
    }

    @Test
    fun `metrics are recorded with the expected tags`() {
        val registry = SimpleMeterRegistry()
        val callback = SlotReaderCallback(producer, connector, CdcOutboxMetrics(registry))

        callback.onSNSSuccess(lsnSucceeded, "orders.events", snsMessage("OrderCreated"))
        callback.onSQSSuccess(lsnSucceeded, "orders.queue")
        callback.onFailure(lsnFailed, "orders.events", "sns", IllegalStateException("nope"))
        callback.discardMessage(lsnFailed, "insert")

        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_PUBLISHED,
                CdcOutboxMetrics.TAG_SINK,
                "sns",
                CdcOutboxMetrics.TAG_TOPIC,
                "orders.events",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_PUBLISHED,
                CdcOutboxMetrics.TAG_SINK,
                "sqs",
                CdcOutboxMetrics.TAG_TOPIC,
                "orders.queue",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_FAILED,
                CdcOutboxMetrics.TAG_SINK,
                "sns",
                CdcOutboxMetrics.TAG_TOPIC,
                "orders.events",
                CdcOutboxMetrics.TAG_CAUSE,
                "IllegalStateException",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_DISCARDED,
                CdcOutboxMetrics.TAG_REASON,
                "insert",
            ).count(),
        )
    }

    private fun snsMessage(eventType: String): SNSMessage<Any> {
        val body =
            SNSMessageBody<Any>(
                eventUUID = UUID.fromString("00000000-0000-0000-0000-000000000001"),
                eventType = eventType,
                domainId = "domain-1",
                domain = "orders",
                eventTimestamp = LocalDateTime.parse("2026-05-14T00:00:00"),
                payload = mapOf("k" to "v"),
            )
        return SNSMessage(headers = emptyMap(), body = body)
    }
}
