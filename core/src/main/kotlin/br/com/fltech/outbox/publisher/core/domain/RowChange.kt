package br.com.fltech.outbox.publisher.core.domain

import java.time.Instant

/**
 * Abstract row-level change event surfaced by a [br.com.fltech.outbox.publisher.core.port.CdcSource]
 * before the [br.com.fltech.outbox.publisher.core.port.MappingRules]
 * port projects it into an [OutboxEvent].
 *
 * The CDC source decides which database-native shape (wal2json `I/U/D`,
 * MySQL binlog row events, outbox-table rows) becomes a [RowChange];
 * the mapping layer then turns it into an [OutboxEvent] according to
 * the user-provided [TableMapping]s.
 *
 * `before` is populated for `UPDATE` and `DELETE`; `after` is populated
 * for `INSERT` and `UPDATE`. Column values are kept as raw `Any?` so
 * adapters can carry through JDBC-native types (Boolean, BigDecimal,
 * Timestamp, ...). The mapping layer is responsible for serialising
 * them into the outbound payload — typically via Jackson.
 */
data class RowChange(
    /** Operation (`INSERT`, `UPDATE`, `DELETE`). */
    val op: Op,
    /** Fully-qualified table name, e.g. `public.orders`. */
    val table: String,
    /** Source-specific checkpoint marker (LSN, GTID, row PK). Opaque to mapping. */
    val sourceCheckpoint: String,
    /** Commit timestamp from the upstream database, if available. */
    val occurredAt: Instant,
    /** Column values before the change (UPDATE + DELETE). */
    val before: Map<String, Any?> = emptyMap(),
    /** Column values after the change (INSERT + UPDATE). */
    val after: Map<String, Any?> = emptyMap(),
) {
    enum class Op { INSERT, UPDATE, DELETE }
}
