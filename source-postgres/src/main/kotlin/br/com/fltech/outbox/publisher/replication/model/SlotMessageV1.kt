package br.com.fltech.outbox.publisher.replication.model

import br.com.fltech.outbox.publisher.replication.model.v1.V1RowRecord
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * wal2json `format-version=1` slot message. Unlike V2 (one record per
 * call), V1 batches every WAL operation of the transaction under the
 * `change` array. Each entry is bound here as a [V1RowRecord] capturing
 * the raw parallel-array wire shape; the V1 parser
 * ([br.com.fltech.outbox.publisher.replication.strategy.ByteToClassParserImplV1])
 * translates each entry into the canonical
 * [br.com.fltech.outbox.publisher.replication.model.Change]
 * subtypes so downstream consumers see the same in-memory
 * representation as V2.
 *
 * `nextlsn` is the WAL position emitted by wal2json when
 * `include-lsn=true`. V1 places the LSN on the wrapper, not on each
 * child record, so the parser copies it down to every translated
 * child. Null when the plugin option is disabled — the producer then
 * falls back to the stream's high-water mark.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class SlotMessageV1
    @JsonCreator
    constructor(
        @param:JsonProperty(value = "xid", required = true)
        val xid: Long,
        @param:JsonProperty(value = "change", required = true)
        val changes: List<V1RowRecord>,
        @param:JsonProperty(value = "nextlsn", required = false)
        val nextLsn: String? = null,
    )
