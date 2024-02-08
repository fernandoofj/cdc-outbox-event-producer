package shop.inventa.pg2sns4k.aws.sns.dto

import shop.inventa.pg2sns4k.aws.common.Payload
import java.time.format.DateTimeFormatter

object SNSMessageMother {

    fun build(): SNSMessage<Payload> {
        val messageBody = SNSMessageBodyMother.build()

        return SNSMessage(
            headers = mapOf(
                Pair("eventType", messageBody.eventType),
                Pair("eventTimestamp", messageBody.eventTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
            ),
            body = messageBody
        )
    }
}
