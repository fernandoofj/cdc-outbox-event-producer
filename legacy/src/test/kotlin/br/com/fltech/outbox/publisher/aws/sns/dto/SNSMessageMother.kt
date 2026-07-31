package br.com.fltech.outbox.publisher.aws.sns.dto

import java.time.format.DateTimeFormatter

object SNSMessageMother {
    fun build(): SNSMessage<Any> {
        val messageBody = SNSMessageBodyMother.build()

        return SNSMessage(
            headers =
                mapOf(
                    Pair("eventType", messageBody.eventType),
                    Pair("eventTimestamp", messageBody.eventTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)),
                ),
            body = messageBody,
        )
    }
}
