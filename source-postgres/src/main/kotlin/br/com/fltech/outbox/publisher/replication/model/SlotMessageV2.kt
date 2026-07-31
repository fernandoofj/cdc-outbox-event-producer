package br.com.fltech.outbox.publisher.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

// LongParameterList: wal2json format-version=2 envelope carries 9
// optional fields by design (M/I/U/D action shapes share one DTO);
// splitting into per-action subtypes loses the single Jackson
// deserialisation path and forces consumers to dispatch twice.

/**
 * wal2json `format-version=2` record. One record per WAL operation
 * (insert, update, delete, message, truncate). The shape varies by
 * `action`:
 *
 *  - `M` (message): `prefix` + `content` + `lsn` are populated; the
 *    row-level fields (`schema`/`table`/`columns`/`identity`) are
 *    absent.
 *  - `I` (insert): `schema`/`table`/`columns` populated; `identity`
 *    absent (the new row IS the row).
 *  - `U` (update): `schema`/`table`/`columns` AND `identity`
 *    populated. `columns` is the post-image; `identity` is the
 *    pre-image projection limited to the replica-identity columns
 *    (typically the primary key).
 *  - `D` (delete): `schema`/`table`/`identity` populated;
 *    `columns` absent (no post-image exists).
 *
 * `columns` and `identity` are surfaced on this DTO so the row-level
 * adapter [br.com.fltech.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource]
 * can emit fully-populated [br.com.fltech.outbox.publisher.core.domain.RowChange]
 * instances. The legacy message-only flow
 * ([br.com.fltech.outbox.publisher.adapter.source.postgres.PgLogicalReplicationCdcSource])
 * still works unchanged — `MessageChange` doesn't look at these
 * fields and `@JsonIgnoreProperties(ignoreUnknown = true)` keeps the
 * existing fixtures parsing.
 */
@Suppress("LongParameterList")
@JsonIgnoreProperties(ignoreUnknown = true)
class SlotMessageV2
    @JsonCreator
    constructor(
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
        /** Source schema for row-level records (I/U/D). Null on `M`. */
        @param:JsonProperty(value = "schema", required = false)
        val schema: String? = null,
        /** Source table for row-level records (I/U/D). Null on `M`. */
        @param:JsonProperty(value = "table", required = false)
        val table: String? = null,
        /**
         * Post-image columns. Populated for `I` and `U`; null for `D` and
         * `M`. Each entry is `{name, type, value}` per wal2json's
         * `format-version=2` schema.
         */
        @param:JsonProperty(value = "columns", required = false)
        val columns: List<Wal2JsonColumn>? = null,
        /**
         * Pre-image projection of the replica-identity columns (typically
         * the primary key). Populated for `U` and `D`; null otherwise.
         */
        @param:JsonProperty(value = "identity", required = false)
        val identity: List<Wal2JsonColumn>? = null,
    )

/**
 * One column entry from a wal2json `format-version=2` row record.
 * `value` is left as a raw `Any?` so JDBC-native types (Int, Long,
 * Boolean, String, nested maps for JSONB) pass through unchanged —
 * the row source materialises them into the
 * [br.com.fltech.outbox.publisher.core.domain.RowChange] `before`/
 * `after` maps without an extra typing pass.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class Wal2JsonColumn
    @JsonCreator
    constructor(
        @param:JsonProperty(value = "name", required = true)
        val name: String,
        @param:JsonProperty(value = "type", required = false)
        val type: String? = null,
        @param:JsonProperty(value = "value", required = false)
        val value: Any? = null,
    )
