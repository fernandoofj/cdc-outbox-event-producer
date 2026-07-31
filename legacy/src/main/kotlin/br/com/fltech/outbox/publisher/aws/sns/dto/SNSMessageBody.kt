package br.com.fltech.outbox.publisher.aws.sns.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.LocalDateTime
import java.util.UUID

data class SNSMessageBody<T>(
    @JsonProperty("eventUUID")
    val eventUUID: UUID,
    @JsonProperty("eventType")
    val eventType: String,
    @JsonProperty("domainId")
    val domainId: String,
    @JsonProperty("domain")
    val domain: String,
    @JsonProperty("eventTimestamp")
    val eventTimestamp: LocalDateTime,
    @JsonProperty("payload")
    val payload: T,
)
