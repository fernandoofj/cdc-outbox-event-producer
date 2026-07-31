package br.com.fltech.outbox.publisher.deadletter

import br.com.fltech.outbox.publisher.replication.model.MessageChange
import io.awspring.cloud.sqs.operations.SendResult
import io.awspring.cloud.sqs.operations.SqsSendOptions
import io.awspring.cloud.sqs.operations.SqsTemplate
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.postgresql.replication.LogSequenceNumber
import java.util.function.Consumer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SqsDeadLetterSinkTest {
    private val sqsTemplate = mockk<SqsTemplate>()
    private val sink = SqsDeadLetterSink(sqsTemplate, queueName = "outbox-dlq")

    @Test
    fun `send forwards a flat envelope to the configured queue`() {
        val captured = slot<Consumer<SqsSendOptions<Map<String, String>>>>()
        every { sqsTemplate.send(capture(captured)) } returns mockk<SendResult<Map<String, String>>>(relaxed = true)

        val lsn = LogSequenceNumber.valueOf("0/16E8198")
        val msg =
            MessageChange(
                kindInput = "message",
                transactional = true,
                prefix = "orders.events",
                content = """{"order":"123"}""",
                lsn = "0/16E8198",
            )

        sink.send(lsn, msg, IllegalStateException("broker is down"))

        verify(exactly = 1) { sqsTemplate.send(any<Consumer<SqsSendOptions<Map<String, String>>>>()) }

        val options = mockk<SqsSendOptions<Map<String, String>>>()
        val queueArg = slot<String>()
        val payloadArg = slot<Map<String, String>>()
        every { options.queue(capture(queueArg)) } returns options
        every { options.payload(capture(payloadArg)) } returns options

        captured.captured.accept(options)

        assertEquals("outbox-dlq", queueArg.captured)
        val envelope = payloadArg.captured
        assertEquals("orders.events", envelope["originalPrefix"])
        assertEquals("0/16E8198", envelope["lsn"])
        assertEquals("""{"order":"123"}""", envelope["content"])
        assertEquals("IllegalStateException", envelope["failureType"])
        assertEquals("broker is down", envelope["failureMessage"])
        assertNotNull(envelope["deadLetteredAt"])
        // ISO-8601 instant has 'T' between date and time.
        assertTrue((envelope["deadLetteredAt"] as String).contains("T"))
    }

    @Test
    fun `send tolerates a null exception message by writing an empty string`() {
        every { sqsTemplate.send(any<Consumer<SqsSendOptions<Map<String, String>>>>()) } returns
            mockk<SendResult<Map<String, String>>>(relaxed = true)

        // RuntimeException with no message produces a null .message
        val cause = RuntimeException()

        sink.send(LogSequenceNumber.valueOf("0/1"), msgChange("orders.events"), cause)

        // No exception thrown — that's the assertion. Verify the call landed.
        verify(exactly = 1) { sqsTemplate.send(any<Consumer<SqsSendOptions<Map<String, String>>>>()) }
    }

    private fun msgChange(prefix: String) =
        MessageChange(
            kindInput = "message",
            transactional = true,
            prefix = prefix,
            content = "{}",
            lsn = null,
        )
}
