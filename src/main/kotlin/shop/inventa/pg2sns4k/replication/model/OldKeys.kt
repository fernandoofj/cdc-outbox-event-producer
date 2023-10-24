package shop.inventa.pg2sns4k.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class OldKeys @JsonCreator constructor(
    @param:JsonProperty(value = "keytypes", required = true)
    val keyTypes: List<String>,
    @JvmField
    @param:JsonProperty(value = "keyvalues", required = true)
    val keyValues: List<Any>,
    @JvmField
    @param:JsonProperty(value = "keynames", required = true)
    val keyNames: List<String>
)
