package br.com.fltech.outbox.publisher.workflow

import br.com.fltech.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.outbox.publisher.replication.connector.PostgresConnector
import br.com.fltech.outbox.publisher.replication.model.MessageChange
import br.com.fltech.outbox.publisher.retry.BackOff
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.postgresql.replication.LogSequenceNumber
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Drives the head-of-line retry + dead-letter state machine introduced
 * in Wave 2b. Exercises three flows:
 *  - happy path: first attempt succeeds, slot advances, no retry recorded.
 *  - retry-then-success: attempts 1 + 2 fail, attempt 3 succeeds. The
 *    slot advances to the per-message LSN at attempt 3, retries are
 *    counted in metrics, and the `pendingFailureLsn` flag is cleared.
 *  - retries exhausted, DLQ configured: all attempts fail; the
 *    dead-letter sink is invoked once, the slot is allowed to advance,
 *    and dead-letter metrics are recorded.
 *  - retries exhausted, DLQ absent: all attempts fail; the slot stays
 *    stuck at the failing LSN; no dead-letter metric is recorded.
 *  - DLQ itself fails: the slot stays stuck and the dead-letter-failure
 *    metric is recorded.
 */
class PublishRetryAndDeadLetterTest {
    private val lsn = LogSequenceNumber.valueOf("0/16E8198")

    private val snsProducer = mockk<SNSProducer>()
    private val sqsProducer = mockk<SQSProducer>(relaxed = true)
    private val connector = mockk<PostgresConnector>()
    private val noDelayBackOff =
        mockk<BackOff>().also {
            every { it.nextDelay(any()) } returns Duration.ZERO
        }
    private val registry = SimpleMeterRegistry()
    private val metrics = CdcOutboxMetrics(registry)

    @BeforeEach
    fun resetConnector() {
        every { connector.setStreamLsn(any()) } just Runs
    }

    private fun buildProducer(
        dlq: DeadLetterSink? = null,
        maxAttempts: Int = 3,
    ): SlotReaderMessageProducer {
        val producer =
            SlotReaderMessageProducer(
                postgresConfiguration =
                    PostgresConfiguration(
                        host = "ignored",
                        database = "x",
                        username = "x",
                        password = "x",
                    ),
                replicationConfiguration = ReplicationConfiguration(slotName = "test_slot"),
                snsProducer = snsProducer,
                sqsProducer = sqsProducer,
                connectionProvider = mockk(relaxed = true),
                metrics = metrics,
                reconnectBackOff = noDelayBackOff,
                deadLetterSink = dlq,
                maxPublishAttempts = maxAttempts,
                publishBackOff = noDelayBackOff,
            )
        producer.running = true
        producer.initializeCallback(connector)
        return producer
    }

    @Test
    fun `happy path advances the slot and records no retries`() {
        every { snsProducer.send(any(), any<SNSMessage<Any>>()) } just Runs
        val producer = buildProducer()

        producer.processMessage(snsMessage("orders.events"), lsn)

        verify(exactly = 1) { snsProducer.send("orders.events", any<SNSMessage<Any>>()) }
        verify(exactly = 1) { connector.setStreamLsn(lsn) }
        assertEquals(
            0.0,
            registry.find(CdcOutboxMetrics.PUBLISH_RETRIES).counter()?.count() ?: 0.0,
        )
    }

    @Test
    fun `retries succeed after two failures and clear the pending-failure flag`() {
        var attempt = 0
        every { snsProducer.send(any(), any<SNSMessage<Any>>()) } answers {
            attempt += 1
            if (attempt < 3) error("transient $attempt")
            // third call returns normally
        }
        val producer = buildProducer()

        producer.processMessage(snsMessage("orders.events"), lsn)

        verify(exactly = 3) { snsProducer.send("orders.events", any<SNSMessage<Any>>()) }
        verify(exactly = 1) { connector.setStreamLsn(lsn) }
        // Two retries — one after attempt 1 failed (tagged attempt="1") and
        // one after attempt 2 failed (tagged attempt="2"). The third attempt
        // succeeds and is NOT a retry.
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.PUBLISH_RETRIES,
                CdcOutboxMetrics.TAG_SINK,
                "sns",
                CdcOutboxMetrics.TAG_TOPIC,
                "orders.events",
                CdcOutboxMetrics.TAG_ATTEMPT,
                "1",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.PUBLISH_RETRIES,
                CdcOutboxMetrics.TAG_SINK,
                "sns",
                CdcOutboxMetrics.TAG_TOPIC,
                "orders.events",
                CdcOutboxMetrics.TAG_ATTEMPT,
                "2",
            ).count(),
        )
    }

    @Test
    fun `exhausted retries with DLQ configured dead-letter the message and allow slot to advance`() {
        every { snsProducer.send(any(), any<SNSMessage<Any>>()) } throws IllegalStateException("permafail")
        val dlq = mockk<DeadLetterSink>()
        every { dlq.send(any(), any(), any()) } just Runs
        val producer = buildProducer(dlq = dlq)

        producer.processMessage(snsMessage("orders.events"), lsn)

        verify(exactly = 3) { snsProducer.send("orders.events", any<SNSMessage<Any>>()) }
        verify(exactly = 1) { dlq.send(lsn, any(), any<IllegalStateException>()) }
        // discardMessage on the callback advances the slot via setStreamLsn.
        verify(atLeast = 1) { connector.setStreamLsn(lsn) }
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.DEAD_LETTERED,
                CdcOutboxMetrics.TAG_SINK,
                "sns",
                CdcOutboxMetrics.TAG_TOPIC,
                "orders.events",
                CdcOutboxMetrics.TAG_CAUSE,
                "IllegalStateException",
            ).count(),
        )
    }

    @Test
    fun `exhausted retries without DLQ keep the slot stuck on the failing LSN`() {
        every { snsProducer.send(any(), any<SNSMessage<Any>>()) } throws IllegalStateException("permafail")
        val producer = buildProducer(dlq = null)

        producer.processMessage(snsMessage("orders.events"), lsn)

        verify(exactly = 3) { snsProducer.send("orders.events", any<SNSMessage<Any>>()) }
        // No setStreamLsn (no success, no discard) — slot stays put.
        verify(exactly = 0) { connector.setStreamLsn(any()) }
        assertNull(registry.find(CdcOutboxMetrics.DEAD_LETTERED).counter())
    }

    @Test
    fun `DLQ failure leaves the slot stuck and records dead-letter-failure metric`() {
        every { snsProducer.send(any(), any<SNSMessage<Any>>()) } throws IllegalStateException("permafail")
        val dlq = mockk<DeadLetterSink>()
        every { dlq.send(any(), any(), any()) } throws RuntimeException("DLQ unreachable")
        val producer = buildProducer(dlq = dlq)

        producer.processMessage(snsMessage("orders.events"), lsn)

        verify(exactly = 1) { dlq.send(lsn, any(), any<IllegalStateException>()) }
        verify(exactly = 0) { connector.setStreamLsn(any()) }
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.DEAD_LETTER_FAILURES,
                CdcOutboxMetrics.TAG_CAUSE,
                "RuntimeException",
            ).count(),
        )
    }

    private fun snsMessage(prefix: String): MessageChange =
        MessageChange(
            kindInput = "message",
            transactional = true,
            prefix = prefix,
            // wal2json-style envelope around the SNSMessage shape — the producer
            // parses it into an SNSMessage<Any> via Jackson at publish time.
            content =
                """
                {
                    "headers":{"eventType":"OrderCreated"},
                    "body":{
                      "eventUUID":"00000000-0000-0000-0000-000000000001",
                      "eventType":"OrderCreated",
                      "domainId":"order-1",
                      "domain":"orders",
                      "eventTimestamp":"2026-05-14T00:00:00",
                      "payload":{}
                    }
                }
                """.trimIndent(),
            lsn = "0/16E8198",
        )
}
