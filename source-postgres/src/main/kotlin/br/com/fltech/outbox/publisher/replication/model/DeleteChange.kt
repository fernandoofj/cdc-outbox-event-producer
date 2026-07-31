package br.com.fltech.outbox.publisher.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * wal2json `DELETE` row record. Only `identity` is populated (the
 * pre-image projection limited to the replica-identity columns —
 * typically the primary key) since deletes have no post-image. All
 * fields default so the legacy V1 deserialisation that surfaces only
 * `kind` keeps working.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class DeleteChange
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
        @param:JsonProperty(value = "identity", required = false)
        val identity: List<Wal2JsonColumn>? = null,
    ) : Change(kindInput)
