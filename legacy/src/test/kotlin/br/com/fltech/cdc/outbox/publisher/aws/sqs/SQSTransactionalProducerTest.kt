package br.com.fltech.cdc.outbox.publisher.aws.sqs

import io.awspring.cloud.sqs.operations.SendResult
import io.awspring.cloud.sqs.operations.SqsSendOptions
import io.awspring.cloud.sqs.operations.SqsTemplate
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.function.Consumer

internal class SQSTransactionalProducerTest {

    private val sqsTemplate = mockk<SqsTemplate>()
    private val producer = SQSTransactionalProducer(sqsTemplate)

    @Test
    fun `send hands SqsTemplate a configurer that sets queue and payload`() {
        val queueName = "queueName"
        val payload = mapOf(
            "field1" to "bla bla bla",
            "field2" to "bla bla bla 2",
        )
        val captured = slot<Consumer<SqsSendOptions<Map<String, String>>>>()
        every { sqsTemplate.send(capture(captured)) } returns mockk<SendResult<Map<String, String>>>(relaxed = true)

        producer.send(queueName, payload)

        verify(exactly = 1) { sqsTemplate.send(any<Consumer<SqsSendOptions<Map<String, String>>>>()) }

        // Replay the captured lambda against a recording mock to confirm
        // queue + payload were applied.
        val options = mockk<SqsSendOptions<Map<String, String>>>()
        every { options.queue(any()) } returns options
        every { options.payload(any()) } returns options
        captured.captured.accept(options)

        verify(exactly = 1) { options.queue(queueName) }
        verify(exactly = 1) { options.payload(payload) }
    }
}
