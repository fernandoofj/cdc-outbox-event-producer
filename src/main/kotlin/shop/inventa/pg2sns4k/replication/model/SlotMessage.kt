package shop.inventa.pg2sns4k.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class SlotMessage @JsonCreator constructor(
    @param:JsonProperty(value = "xid", required = true)
    val xid: Long,
    @param:JsonProperty(value = "change", required = true)
    val changes: List<Change>
)
