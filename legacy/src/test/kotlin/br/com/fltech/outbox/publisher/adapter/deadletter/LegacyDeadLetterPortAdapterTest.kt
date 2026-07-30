package br.com.fltech.outbox.publisher.adapter.deadletter

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.outbox.publisher.replication.model.MessageChange
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.postgresql.replication.LogSequenceNumber
import java.time.Instant
import kotlin.test.assertEquals

class LegacyDeadLetterPortAdapterTest {

    @Test
    fun `send reconstructs MessageChange and LogSequenceNumber from the OutboxEvent`() {
        val legacy = mockk<DeadLetterSink>()
        val msgCapture = slot<MessageChange>()
        every { legacy.send(any(), capture(msgCapture), any()) } just Runs

        val event = OutboxEvent(
            id = "0/16E8198",
            routing = Routing("kafka", "orders/cluster-a"),
            payload = """{"order":1}""".toByteArray(Charsets.UTF_8),
            occurredAt = Instant.EPOCH,
            sourceCheckpoint = "0/16E8198",
        )
        val cause = IllegalStateException("boom")

        LegacyDeadLetterPortAdapter(legacy).send(event, cause)

        verify(exactly = 1) {
            legacy.send(
                lsn = LogSequenceNumber.valueOf("0/16E8198"),
                messageChange = any(),
                lastException = cause,
            )
        }
        val msg = msgCapture.captured
        assertEquals("kafka://orders/cluster-a", msg.prefix)
        assertEquals("""{"order":1}""", msg.content)
        assertEquals("0/16E8198", msg.lsn)
    }

    @Test
    fun `send tolerates a non-LSN sourceCheckpoint by passing INVALID_LSN`() {
        val legacy = mockk<DeadLetterSink>()
        every { legacy.send(any(), any(), any()) } just Runs

        val event = OutboxEvent(
            id = "row-42",
            routing = Routing("amqp", "exchange/key"),
            payload = ByteArray(0),
            occurredAt = Instant.EPOCH,
            sourceCheckpoint = "row-42", // MySQL-style; not an LSN
        )

        LegacyDeadLetterPortAdapter(legacy).send(event, RuntimeException())

        // INVALID_LSN is what the legacy sink will see; it serialises to "0/0" in the DLQ envelope.
        verify(exactly = 1) {
            legacy.send(
                lsn = LogSequenceNumber.INVALID_LSN,
                messageChange = any(),
                lastException = any(),
            )
        }
    }
}
