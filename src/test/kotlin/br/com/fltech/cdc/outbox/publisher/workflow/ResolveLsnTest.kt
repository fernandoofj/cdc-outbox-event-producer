package br.com.fltech.cdc.outbox.publisher.workflow

import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.cdc.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.postgresql.replication.LogSequenceNumber
import kotlin.test.assertEquals

/**
 * Focused unit tests for [SlotReaderMessageProducer.resolveLsn] — the small
 * pure function that decides whether the per-message wal2json `lsn` field
 * is usable or whether the stream's high-water-mark fallback should be
 * preferred. Exercises:
 *  - normal path: a well-formed `X/X` hex pair is parsed.
 *  - missing field: returns the fallback unchanged.
 *  - malformed field: warn-and-fallback path.
 */
class ResolveLsnTest {

    private val producer = SlotReaderMessageProducer(
        postgresConfiguration = PostgresConfiguration(
            host = "ignored",
            database = "ignored",
            username = "ignored",
            password = "ignored",
        ),
        replicationConfiguration = ReplicationConfiguration(slotName = "ignored"),
        snsProducer = mockk<SNSProducer>(relaxed = true),
        sqsProducer = mockk<SQSProducer>(relaxed = true),
        connectionProvider = mockk(relaxed = true),
    )

    private val fallback = LogSequenceNumber.valueOf(0xC0FFEEL)

    @Test
    fun `well-formed embedded LSN is preferred over the fallback`() {
        val parsed = producer.resolveLsn(messageWithLsn("0/16E8198"), fallback)
        assertEquals(LogSequenceNumber.valueOf("0/16E8198"), parsed)
    }

    @Test
    fun `missing LSN field falls back to the captured stream LSN`() {
        val parsed = producer.resolveLsn(messageWithLsn(null), fallback)
        assertEquals(fallback, parsed)
    }

    @Test
    fun `malformed LSN warns and falls back to the captured stream LSN`() {
        val parsed = producer.resolveLsn(messageWithLsn("not-an-lsn-string"), fallback)
        assertEquals(fallback, parsed)
    }

    private fun messageWithLsn(lsn: String?): MessageChange = MessageChange(
        kindInput = "message",
        transactional = true,
        prefix = "topic-a",
        content = "{}",
        lsn = lsn,
    )
}
