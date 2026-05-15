package shop.inventa.pg2sns4k.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
class SlotMessageV2 @JsonCreator constructor(
    @param:JsonProperty(value = "xid", required = true)
    val xid: Long,
    @param:JsonProperty(value = "action", required = true)
    val action: String,
    @param:JsonProperty(value = "prefix", required = false)
    val prefix: String? = null,
    @param:JsonProperty(value = "content", required = false)
    val content: String? = null
)
