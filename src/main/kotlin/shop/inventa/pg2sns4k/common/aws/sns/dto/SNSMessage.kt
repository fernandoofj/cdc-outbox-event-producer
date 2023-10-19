package shop.inventa.pg2sns4k.common.aws.sns.dto

import com.fasterxml.jackson.annotation.JsonProperty

data class SNSMessage<T>(
    @JsonProperty("headers")
    val headers: Map<String, String>,
    @JsonProperty("body")
    val body: SNSMessageBody<T>
)
