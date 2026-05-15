package br.com.fltech.cdc.outbox.publisher.aws.sns

import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessageBody
import io.awspring.cloud.sns.core.SnsTemplate
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

internal class SNSTransactionalProducerTest {

    private val snsTemplate = mockk<SnsTemplate>()
    private val producer = SNSTransactionalProducer(snsTemplate)

    @Test
    fun `send delegates to SnsTemplate convertAndSend with topic, body and headers`() {
        val topicName = "topicName"
        val messageBody = SNSMessageBody(
            eventUUID = UUID.randomUUID(),
            eventType = "eventType",
            domainId = UUID.randomUUID().toString(),
            domain = "domain",
            eventTimestamp = LocalDateTime.now(),
            payload = mapOf(
                "field1" to "bla bla bla",
                "field2" to "bla bla bla 2",
            ),
        )
        val message = SNSMessage(
            headers = mapOf(
                "eventType" to messageBody.eventType,
                "eventTimestamp" to messageBody.eventTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
            ),
            body = messageBody,
        )

        every { snsTemplate.convertAndSend(topicName, message.body, message.headers) } just Runs

        producer.send(topicName, message)

        verify(exactly = 1) {
            snsTemplate.convertAndSend(topicName, message.body, message.headers)
        }
    }
}
