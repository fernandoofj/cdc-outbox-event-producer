package br.com.fltech.cdc.outbox.publisher.replication.model

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
    val content: String? = null,
    /**
     * LSN of THIS WAL record as emitted by `wal2json` when
     * `include-lsn=true`. Format is the standard Postgres `X/X` hex pair,
     * e.g. `0/16E8198`. May be null if the plugin option is disabled.
     */
    @param:JsonProperty(value = "lsn", required = false)
    val lsn: String? = null,
)
