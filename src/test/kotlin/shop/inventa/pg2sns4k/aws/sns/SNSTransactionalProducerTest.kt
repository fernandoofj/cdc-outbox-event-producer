package shop.inventa.pg2sns4k.aws.sns

import io.awspring.cloud.messaging.core.NotificationMessagingTemplate
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessageBody
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

@ExtendWith(MockKExtension::class)
internal class SNSTransactionalProducerTest {

    @MockK
    private lateinit var notificationMessagingTemplate: NotificationMessagingTemplate

    @InjectMockKs
    private lateinit var snsTransactionalProducer: shop.inventa.pg2sns4k.aws.sns.SNSTransactionalProducer

    @Test
    fun `send should produce a message`() {
        // given
        val topicName = "topicName"
        val messageBody = SNSMessageBody(
            eventUUID = UUID.randomUUID(),
            eventType = "eventType",
            domainId = UUID.randomUUID().toString(),
            domain = "domain",
            eventTimestamp = LocalDateTime.now(),
            payload = mapOf(
                Pair("field1", "bla bla bla"),
                Pair("field2", "bla bla bla 2")
            )
        )
        val message = SNSMessage(
            headers = mapOf(
                Pair("eventType", messageBody.eventType),
                Pair("eventTimestamp", messageBody.eventTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
            ),
            body = messageBody
        )

        every {
            notificationMessagingTemplate.convertAndSend(topicName, message.body, message.headers)
        } just runs

        // when
        snsTransactionalProducer.send(topicName, message)

        // then
        verify(exactly = 1) {
            notificationMessagingTemplate.convertAndSend(
                any<String>(),
                any<Any>(),
                any<Map<String, Any>>()
            )
        }
    }
}
