package br.com.fltech.cdc.outbox.publisher.replication.model

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
