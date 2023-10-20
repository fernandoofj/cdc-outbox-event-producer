package shop.inventa.pg2sns4k.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class MessageChange @JsonCreator constructor(
    @JsonProperty(value = "kind", required = true)
    private val kindInput: String,
    @param:JsonProperty(value = "transactional", required = true)
    val transactional: Boolean,
    @param:JsonProperty(value = "prefix", required = true)
    val prefix: String,
    @param:JsonProperty(value = "content", required = true)
    val content: String
) : Change(kindInput)
