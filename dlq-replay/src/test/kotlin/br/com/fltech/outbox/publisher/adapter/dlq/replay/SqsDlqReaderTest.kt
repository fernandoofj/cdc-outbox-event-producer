package br.com.fltech.outbox.publisher.adapter.dlq.replay

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse
import software.amazon.awssdk.services.sqs.model.Message
import software.amazon.awssdk.services.sqs.model.QueueAttributeName
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse

class SqsDlqReaderTest {

    private val sqs = mockk<SqsClient>(relaxed = true)
    private val queueName = "cdc-outbox-dlq"
    private val queueUrl = "https://sqs.local/$queueName"
    private val mapper = jacksonObjectMapper()

    init {
        every { sqs.getQueueUrl(any<GetQueueUrlRequest>()) } returns
            GetQueueUrlResponse.builder().queueUrl(queueUrl).build()
    }

    @Test
    fun `peek returns empty list when SQS has no messages`() {
        every { sqs.receiveMessage(any<ReceiveMessageRequest>()) } returns
            ReceiveMessageResponse.builder().messages(emptyList()).build()

        val reader = SqsDlqReader(sqs, queueName)

        assertEquals(emptyList(), reader.peek(10))
    }

    @Test
    fun `peek deserialises envelopes and preserves receipt handles`() {
        val envelope = sampleEnvelope()
        val body = mapper.writeValueAsString(envelope)
        every { sqs.receiveMessage(any<ReceiveMessageRequest>()) } returns
            ReceiveMessageResponse.builder()
                .messages(
                    Message.builder().receiptHandle("AQEB-handle-1").body(body).messageId("m1").build(),
                )
                .build()

        val reader = SqsDlqReader(sqs, queueName)
        val messages = reader.peek(10)

        assertEquals(1, messages.size)
        assertEquals("AQEB-handle-1", messages[0].handle.value)
        assertEquals(envelope.originalPrefix, messages[0].envelope.originalPrefix)
        assertEquals(envelope.lsn, messages[0].envelope.lsn)
        assertEquals(envelope.content, messages[0].envelope.content)
    }

    @Test
    fun `peek clamps batch size into SQS allowed range`() {
        every { sqs.receiveMessage(any<ReceiveMessageRequest>()) } returns
            ReceiveMessageResponse.builder().messages(emptyList()).build()

        val reader = SqsDlqReader(sqs, queueName)
        reader.peek(max = 1000)

        verify {
            sqs.receiveMessage(
                match<ReceiveMessageRequest> { req -> req.maxNumberOfMessages() == 10 },
            )
        }
    }

    @Test
    fun `peek drops malformed envelope rather than aborting the batch`() {
        val good = sampleEnvelope()
        val goodBody = mapper.writeValueAsString(good)
        every { sqs.receiveMessage(any<ReceiveMessageRequest>()) } returns
            ReceiveMessageResponse.builder()
                .messages(
                    Message.builder().receiptHandle("h1").body("{not-json").messageId("m1").build(),
                    Message.builder().receiptHandle("h2").body(goodBody).messageId("m2").build(),
                )
                .build()

        val reader = SqsDlqReader(sqs, queueName)
        val messages = reader.peek(10)

        // Malformed entry was logged + dropped; the well-formed one came through.
        assertEquals(1, messages.size)
        assertEquals("h2", messages[0].handle.value)
    }

    @Test
    fun `peek tolerates extra unknown fields in the envelope JSON`() {
        // @JsonIgnoreProperties guarantees forward-compat when
        // the writer adds fields the reader does not know about.
        val withExtras = """
            {
              "originalPrefix": "sns://orders-events",
              "lsn": "0/16E8198",
              "content": "{\"orderId\":42}",
              "failureType": "TimeoutException",
              "failureMessage": "publish timed out",
              "deadLetteredAt": "2026-05-15T10:00:00Z",
              "newFieldFromFuture": "ignored"
            }
        """.trimIndent()
        every { sqs.receiveMessage(any<ReceiveMessageRequest>()) } returns
            ReceiveMessageResponse.builder()
                .messages(Message.builder().receiptHandle("h").body(withExtras).build())
                .build()

        val reader = SqsDlqReader(sqs, queueName)

        assertEquals(1, reader.peek(10).size)
    }

    @Test
    fun `delete propagates SQS failure rather than swallowing it`() {
        every { sqs.deleteMessage(any<DeleteMessageRequest>()) } throws
            QueueDoesNotExistException.builder().message("missing").build()

        val reader = SqsDlqReader(sqs, queueName)

        assertThrows<QueueDoesNotExistException> {
            reader.delete(DlqReader.Handle("unknown-handle"))
        }
    }

    @Test
    fun `stats returns the SQS approximate counters`() {
        every { sqs.getQueueAttributes(any<GetQueueAttributesRequest>()) } returns
            GetQueueAttributesResponse.builder()
                .attributes(
                    mapOf(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES to "42",
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE to "3",
                    ),
                )
                .build()

        val reader = SqsDlqReader(sqs, queueName)
        val stats = reader.stats()

        assertEquals(42L, stats.approximateMessageCount)
        assertEquals(3L, stats.approximateMessageNotVisibleCount)
    }

    @Test
    fun `stats returns zero when SQS returns no attributes`() {
        every { sqs.getQueueAttributes(any<GetQueueAttributesRequest>()) } returns
            GetQueueAttributesResponse.builder().attributes(emptyMap()).build()

        val reader = SqsDlqReader(sqs, queueName)
        val stats = reader.stats()

        assertEquals(0L, stats.approximateMessageCount)
        assertTrue(stats.approximateMessageNotVisibleCount == 0L)
    }

    private fun sampleEnvelope(
        prefix: String = "sns://orders-events",
        lsn: String = "0/16E8198",
    ) = DlqEnvelope(
        originalPrefix = prefix,
        lsn = lsn,
        content = """{"orderId":42}""",
        failureType = "TimeoutException",
        failureMessage = "publish timed out",
        deadLetteredAt = "2026-05-15T10:00:00Z",
    )
}
