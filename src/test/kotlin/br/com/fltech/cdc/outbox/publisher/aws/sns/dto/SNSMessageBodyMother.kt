package br.com.fltech.cdc.outbox.publisher.aws.sns.dto

import br.com.fltech.cdc.outbox.publisher.aws.common.PayloadMother
import java.time.LocalDateTime
import java.util.UUID

object SNSMessageBodyMother {

    fun build() = SNSMessageBody(
        eventType = "PRODUCT_CREATED",
        eventUUID = UUID.randomUUID(),
        eventTimestamp = LocalDateTime.now(),
        domainId = UUID.randomUUID().toString(),
        domain = "CATALOGUE",
        payload = PayloadMother.build() as Any
    )
}
