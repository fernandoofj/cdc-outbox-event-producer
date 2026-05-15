package br.com.fltech.cdc.outbox.publisher.adapter.source.postgres

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.port.CheckpointStore
import br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.cdc.outbox.publisher.replication.connector.PostgresConnector
import br.com.fltech.cdc.outbox.publisher.replication.model.DeleteChange
import br.com.fltech.cdc.outbox.publisher.replication.model.InsertChange
import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import br.com.fltech.cdc.outbox.publisher.replication.model.UpdateChange
import br.com.fltech.cdc.outbox.publisher.replication.model.Wal2JsonColumn
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV1
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV2
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserStrategy
import com.fasterxml.jackson.databind.ObjectMapper
import org.postgresql.replication.LogSequenceNumber
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Postgres row-level [RowChangeSource] backed by the same
 * [PostgresConnector] / wal2json machinery as
 * [PgLogicalReplicationCdcSource]. Where that adapter only surfaces
 * `pg_logical_emit_message` (`M`) records and feeds [CdcSource]
 * directly, this one streams **row-level** WAL records (`I` / `U` /
 * `D`) so the Wave 3.5 mapping infra applies to Postgres on equal
 * footing with MySQL binlog.
 *
 * The two coexist: a deployment that only emits transactional
 * messages keeps using `PgLogicalReplicationCdcSource`; a deployment
 * that wants table-driven mapping wires this one. They MUST NOT run
 * concurrently on the same slot — each `PostgresConnector` opens its
 * own replication stream and `setStreamLsn` from both would race.
 *
 * Checkpoint format: the per-record LSN as returned by
 * `LogSequenceNumber.asString()` (e.g. `0/16E8198`). Same shape the
 * legacy adapter uses, so a [CheckpointStore] value is portable
 * across the two if you migrate.
 *
 * On `ack`:
 *  1. Advances the replication slot's flushed LSN via
 *     `connector.setStreamLsn(...)` so the WAL can be reclaimed.
 *  2. Persists the LSN through the optional [CheckpointStore] so the
 *     adapter can resume cleanly on restart — symmetric with
 *     [br.com.fltech.cdc.outbox.publisher.adapter.source.mysql.MySqlBinlogRowChangeSource].
 *     Note: Postgres also persists the LSN inside the slot itself, so
 *     the store is informational here (useful for diagnostics and
 *     parity with the MySQL flow).
 *
 * `MessageChange` records (the legacy `pg_logical_emit_message`
 * path) are ignored — that flow has its own adapter. If both this
 * adapter and `PgLogicalReplicationCdcSource` were ever wired
 * against the same slot a single record could be observed twice;
 * the auto-config gates them mutually exclusive.
 *
 * Single-threaded contract per [RowChangeSource] holds.
 */
// TooManyFunctions: one method per wal2json change type (Insert,
// Update, Delete, Message) plus lifecycle (open/poll/ack/close) plus
// internal helpers (toRowChange, parseLsnOrNull, checkOpen, buffer
// management). Splitting hurts the adapter-reader's ability to
// follow the dispatch — the class IS the adapter for one wire format.
@Suppress("TooManyFunctions")
class PgWalRowChangeSource(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val connectionProvider: ConnectionProvider,
    objectMapper: ObjectMapper,
    private val checkpointStore: CheckpointStore? = null,
    /**
     * Factory hook for the underlying [PostgresConnector]. Overridable
     * in tests so the constructor doesn't open real replication
     * connections — same pattern as the MySQL binlog adapter's
     * `clientFactory`.
     */
    private val connectorFactory:
        (PostgresConfiguration, ReplicationConfiguration, ConnectionProvider) -> PostgresConnector =
            { p, r, c -> PostgresConnector(p, r, c) },
) : RowChangeSource {

    private val parser = ByteToClassParserStrategy(
        ByteToClassParserImplV1(objectMapper),
        ByteToClassParserImplV2(objectMapper),
    ).selectParser(replicationConfiguration)

    private val opened = AtomicBoolean(false)
    private var connector: PostgresConnector? = null

    /**
     * Per WAL chunk wal2json may emit multiple change records (V1
     * batched format); we drain them into this buffer so `poll`'s
     * "one [RowChange] per call" contract is satisfied without
     * losing records.
     */
    private val buffer = ArrayDeque<RowChange>()

    override fun open() {
        if (!opened.compareAndSet(false, true)) {
            logger.debug("PgWalRowChangeSource already opened; ignoring duplicate open()")
            return
        }
        // The persisted checkpoint is informational here — Postgres
        // persists the slot's confirmed_flush_lsn server-side, so a
        // restart already resumes at the right LSN without reading
        // this. Logging is enough so the operator sees the parity
        // with the MySQL flow.
        checkpointStore?.let { store ->
            runCatching { store.load(checkpointKey()) }
                .onFailure {
                    logger.warn(
                        "CheckpointStore.load failed for key={} ({}); slot LSN authoritative on Postgres.",
                        checkpointKey(), it.javaClass.simpleName, it,
                    )
                }
                .onSuccess { persisted ->
                    if (persisted != null) {
                        logger.info(
                            "PgWalRowChangeSource: persisted checkpoint key={} value={} (slot LSN authoritative)",
                            checkpointKey(), persisted,
                        )
                    }
                }
        }
        connector = connectorFactory(postgresConfiguration, replicationConfiguration, connectionProvider)
        logger.info("PgWalRowChangeSource opened on slot '{}'", replicationConfiguration.slotName)
    }

    // ReturnCount: 3 distinct outcomes (buffered residue, nothing read,
    // freshly-parsed batch). Single-return would have to inline the
    // batch-parsing block behind a non-empty check and obscure the
    // buffered fast path.
    @Suppress("ReturnCount")
    override fun poll(): RowChange? {
        if (buffer.isNotEmpty()) return buffer.poll()
        val conn = checkOpen()
        val byteBuffer = conn.readPending() ?: return null
        val fallbackLsn = conn.lastReceivedLsn()
        val changes = parser.parse(byteBuffer)
        for (change in changes) {
            when (change) {
                is InsertChange -> toRowChange(change, fallbackLsn)?.let(buffer::offer)
                is UpdateChange -> toRowChange(change, fallbackLsn)?.let(buffer::offer)
                is DeleteChange -> toRowChange(change, fallbackLsn)?.let(buffer::offer)
                is MessageChange -> Unit // owned by PgLogicalReplicationCdcSource
                else -> Unit
            }
        }
        return buffer.poll()
    }

    override fun ack(rowChange: RowChange) {
        val conn = checkOpen()
        val lsn = parseLsnOrNull(rowChange.sourceCheckpoint)
        if (lsn != null) {
            conn.setStreamLsn(lsn)
        }
        checkpointStore?.let { store ->
            try {
                store.save(checkpointKey(), rowChange.sourceCheckpoint)
            } catch (e: Exception) {
                logger.warn(
                    "CheckpointStore.save failed for key={} value={} ({}); slot LSN still advanced.",
                    checkpointKey(), rowChange.sourceCheckpoint, e.javaClass.simpleName, e,
                )
            }
        }
    }

    override fun close() {
        if (!opened.compareAndSet(true, false)) return
        runCatching { connector?.close() }
        connector = null
        buffer.clear()
        logger.info("PgWalRowChangeSource closed for slot '{}'", replicationConfiguration.slotName)
    }

    private fun checkOpen(): PostgresConnector = connector
        ?: error("PgWalRowChangeSource must be opened before use")

    private fun toRowChange(change: InsertChange, fallbackLsn: LogSequenceNumber): RowChange? {
        val table = qualifiedTable(change.schema, change.table) ?: return null
        val lsn = resolveLsn(change.lsn, fallbackLsn).asString()
        return RowChange(
            op = RowChange.Op.INSERT,
            table = table,
            sourceCheckpoint = lsn,
            occurredAt = Instant.now(),
            after = columnsToMap(change.columns),
        )
    }

    private fun toRowChange(change: UpdateChange, fallbackLsn: LogSequenceNumber): RowChange? {
        val table = qualifiedTable(change.schema, change.table) ?: return null
        val lsn = resolveLsn(change.lsn, fallbackLsn).asString()
        return RowChange(
            op = RowChange.Op.UPDATE,
            table = table,
            sourceCheckpoint = lsn,
            occurredAt = Instant.now(),
            before = columnsToMap(change.identity),
            after = columnsToMap(change.columns),
        )
    }

    private fun toRowChange(change: DeleteChange, fallbackLsn: LogSequenceNumber): RowChange? {
        val table = qualifiedTable(change.schema, change.table) ?: return null
        val lsn = resolveLsn(change.lsn, fallbackLsn).asString()
        return RowChange(
            op = RowChange.Op.DELETE,
            table = table,
            sourceCheckpoint = lsn,
            occurredAt = Instant.now(),
            before = columnsToMap(change.identity),
        )
    }

    private fun qualifiedTable(schema: String?, table: String?): String? {
        if (table.isNullOrBlank()) {
            // wal2json V2 should always carry schema+table on I/U/D.
            // A missing value means either the plugin is misconfigured
            // or the record is malformed — surface and drop.
            logger.warn("PgWalRowChangeSource: row record missing table name; dropping change.")
            return null
        }
        return if (schema.isNullOrBlank()) table else "$schema.$table"
    }

    private fun columnsToMap(columns: List<Wal2JsonColumn>?): Map<String, Any?> {
        if (columns.isNullOrEmpty()) return emptyMap()
        // LinkedHashMap preserves wal2json's column order so downstream
        // mapping rules that rely on ordering (e.g. composite-key
        // templates) see the same shape the upstream emits.
        val out = LinkedHashMap<String, Any?>(columns.size)
        for (c in columns) {
            out[c.name] = c.value
        }
        return out
    }

    private fun resolveLsn(embedded: String?, fallback: LogSequenceNumber): LogSequenceNumber {
        if (embedded.isNullOrBlank()) return fallback
        val parsed = LogSequenceNumber.valueOf(embedded)
        return if (parsed == LogSequenceNumber.INVALID_LSN) {
            logger.warn(
                "Embedded wal2json lsn '{}' parsed as INVALID_LSN; falling back to stream LSN {}",
                embedded, fallback,
            )
            fallback
        } else {
            parsed
        }
    }

    private fun parseLsnOrNull(s: String): LogSequenceNumber? {
        val parsed = LogSequenceNumber.valueOf(s)
        return if (parsed == LogSequenceNumber.INVALID_LSN) null else parsed
    }

    private fun checkpointKey(): String = "pg-wal:${replicationConfiguration.slotName}"

    companion object {
        private val logger = LoggerFactory.getLogger(PgWalRowChangeSource::class.java)
    }
}
