package br.com.fltech.outbox.publisher.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * wal2json `UPDATE` row record. `columns` is the post-image of the
 * updated row; `identity` is the pre-image projection limited to the
 * replica-identity columns (typically the primary key) — that's the
 * upstream's `before` view. All fields are nullable + default to keep
 * the legacy V1 deserialisation that surfaces only `kind`.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class UpdateChange
    @JsonCreator
    constructor(
        @JsonProperty(value = "kind", required = true)
        private val kindInput: String,
        @param:JsonProperty(value = "schema", required = false)
        val schema: String? = null,
        @param:JsonProperty(value = "table", required = false)
        val table: String? = null,
        @param:JsonProperty(value = "lsn", required = false)
        val lsn: String? = null,
        @param:JsonProperty(value = "columns", required = false)
        val columns: List<Wal2JsonColumn>? = null,
        @param:JsonProperty(value = "identity", required = false)
        val identity: List<Wal2JsonColumn>? = null,
    ) : Change(kindInput)
