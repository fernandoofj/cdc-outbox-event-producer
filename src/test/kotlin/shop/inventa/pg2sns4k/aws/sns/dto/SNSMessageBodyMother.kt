package shop.inventa.pg2sns4k.aws.sns.dto

import shop.inventa.pg2sns4k.aws.common.Payload
import shop.inventa.pg2sns4k.aws.common.PayloadMother
import java.time.LocalDateTime
import java.util.UUID

object SNSMessageBodyMother {

    fun build() = SNSMessageBody(
        eventType = "PRODUCT_CREATED",
        eventUUID = UUID.randomUUID(),
        eventTimestamp = LocalDateTime.now(),
        domainId = UUID.randomUUID().toString(),
        domain = "CATALOGUE",
        payload = PayloadMother.build()
    )
}
