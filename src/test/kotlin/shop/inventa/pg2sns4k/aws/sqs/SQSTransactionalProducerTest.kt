package shop.inventa.pg2sns4k.aws.sqs

import io.awspring.cloud.messaging.core.QueueMessagingTemplate
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
internal class SQSTransactionalProducerTest {

    @MockK
    private lateinit var queueMessagingTemplate: QueueMessagingTemplate

    @InjectMockKs
    private lateinit var sqsTransactionalProducer: SQSTransactionalProducer

    @Test
    fun `send should produce a message`() {
        // given
        val queueName = "queueName"
        val payload = mapOf(
            Pair("field1", "bla bla bla"),
            Pair("field2", "bla bla bla 2")
        )

        every {
            queueMessagingTemplate.convertAndSend(queueName, payload)
        } just runs

        // when
        sqsTransactionalProducer.send(queueName, payload)

        // then
        verify(exactly = 1) {
            queueMessagingTemplate.convertAndSend(
                any<String>(),
                any<Map<String, String>>()
            )
        }
    }
}
