package br.com.fltech.cdc.outbox.publisher.replication.model.v1

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Raw wal2json `format-version=1` record. The same wrapper holds two
 * very different shapes — row records (`insert` / `update` / `delete`)
 * and message records (`pg_logical_emit_message`, `kind=message`) —
 * so this DTO is a permissive union with everything nullable. The V1
 * parser dispatches on `kind` and pulls only the fields that are
 * meaningful for that variant.
 *
 * Row variants emit three parallel arrays —
 * `columnnames` / `columntypes` / `columnvalues` — for the post-image
 * and a nested `oldkeys` wrapper for the pre-image projection on the
 * replica identity. V2 carries the equivalent data as a single
 * `columns: List<{name,type,value}>` array per record.
 *
 * Message variants carry `prefix` / `content` / `transactional`
 * directly on the record — the same trio V2 surfaces on
 * [br.com.fltech.cdc.outbox.publisher.replication.model.SlotMessageV2].
 *
 * The V1 parser
 * ([br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV1])
 * translates each entry into the canonical
 * [br.com.fltech.cdc.outbox.publisher.replication.model.InsertChange] /
 * [br.com.fltech.cdc.outbox.publisher.replication.model.UpdateChange] /
 * [br.com.fltech.cdc.outbox.publisher.replication.model.DeleteChange] /
 * [br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange]
 * subtype so consumers downstream see the same in-memory
 * representation as V2.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class V1RowRecord @JsonCreator constructor(
    @param:JsonProperty(value = "kind", required = true)
    val kind: String,
    @param:JsonProperty(value = "schema", required = false)
    val schema: String? = null,
    @param:JsonProperty(value = "table", required = false)
    val table: String? = null,
    @param:JsonProperty(value = "columnnames", required = false)
    val columnNames: List<String>? = null,
    @param:JsonProperty(value = "columntypes", required = false)
    val columnTypes: List<String?>? = null,
    @param:JsonProperty(value = "columnvalues", required = false)
    val columnValues: List<Any?>? = null,
    @param:JsonProperty(value = "oldkeys", required = false)
    val oldKeys: V1OldKeys? = null,
    /** `pg_logical_emit_message` transactional flag (message records only). */
    @param:JsonProperty(value = "transactional", required = false)
    val transactional: Boolean? = null,
    /** `pg_logical_emit_message` user-supplied prefix (message records only). */
    @param:JsonProperty(value = "prefix", required = false)
    val prefix: String? = null,
    /** `pg_logical_emit_message` user-supplied content (message records only). */
    @param:JsonProperty(value = "content", required = false)
    val content: String? = null,
)

/**
 * V1 replica-identity projection nested under each `UPDATE` / `DELETE`
 * row record. Same parallel-array layout as the post-image fields on
 * [V1RowRecord], scoped to the replica-identity columns (typically the
 * primary key). All arrays default to `null` so a malformed or partial
 * `oldkeys` block degrades to an empty identity instead of blowing up
 * the parser.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class V1OldKeys @JsonCreator constructor(
    @param:JsonProperty(value = "keynames", required = false)
    val keyNames: List<String>? = null,
    @param:JsonProperty(value = "keytypes", required = false)
    val keyTypes: List<String?>? = null,
    @param:JsonProperty(value = "keyvalues", required = false)
    val keyValues: List<Any?>? = null,
)
