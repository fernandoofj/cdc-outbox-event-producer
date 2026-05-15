package br.com.fltech.cdc.outbox.publisher.replication.model

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
    val content: String,
    /**
     * Per-message LSN parsed from the wal2json `lsn` field (when
     * `include-lsn=true`). When present, this is the LSN of THIS WAL record
     * and is used to drive `setStreamLsn` after a successful publish. When
     * absent, the producer falls back to `PGReplicationStream.lastReceiveLSN`
     * with the known drift caveat.
     */
    @param:JsonProperty(value = "lsn", required = false)
    val lsn: String? = null,
) : Change(kindInput)
