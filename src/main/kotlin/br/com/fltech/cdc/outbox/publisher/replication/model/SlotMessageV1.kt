package br.com.fltech.cdc.outbox.publisher.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
class SlotMessageV1 @JsonCreator constructor(
    @param:JsonProperty(value = "xid", required = true)
    val xid: Long,
    @param:JsonProperty(value = "change", required = true)
    val changes: List<Change>
)
