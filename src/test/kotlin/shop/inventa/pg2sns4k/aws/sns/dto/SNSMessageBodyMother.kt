package shop.inventa.pg2sns4k.aws.sns.dto

import java.time.LocalDateTime
import java.util.UUID

object SNSMessageBodyMother {

    fun build() = SNSMessageBody<Map<String, Any>> (
        eventType = "PRODUCT_CREATED",
        eventUUID = UUID.randomUUID(),
        eventTimestamp = LocalDateTime.now(),
        domainId = UUID.randomUUID().toString(),
        domain = "CATALOGUE",
        payload = mapOf(
            Pair("id", UUID.randomUUID().toString()),
            Pair("name", "Product name"),
            Pair("description", "Product description"),
            Pair("price", 100.0),
            Pair("category", "Category")
        )
    )
}
