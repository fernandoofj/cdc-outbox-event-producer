package br.com.fltech.cdc.outbox.publisher.adapter.source.mysql

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData
import com.github.shyiko.mysql.binlog.event.EventType
import com.github.shyiko.mysql.binlog.event.QueryEventData
import com.github.shyiko.mysql.binlog.event.RotateEventData
import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

/**
 * MySQL [RowChangeSource] backed by Stanley Shyiko's
 * `mysql-binlog-connector-java`. Streams `WRITE_ROWS` / `UPDATE_ROWS`
 * / `DELETE_ROWS` events from the binary log, translates each row
 * into a [RowChange] and offers them via the non-blocking `poll`
 * contract of [RowChangeSource].
 *
 * Setup requirements on the MySQL side:
 *  - `binlog_format=ROW`
 *  - `binlog_row_metadata=FULL` (so column names are recoverable; on
 *    earlier MySQL builds without this, column names are reported as
 *    `col0`, `col1`, …)
 *  - `binlog_row_image=FULL` (so DELETE events carry the full before
 *    image; otherwise only the primary key is reported)
 *  - the connecting user needs `REPLICATION SLAVE` and
 *    `REPLICATION CLIENT`.
 *
 * Column-name resolution: when a [DataSource] is provided the adapter
 * queries `information_schema.columns` once per `tableId` (on the
 * first `TABLE_MAP` event for that id) to map row indices to real
 * column names. The lookup is cached for the rest of the source's
 * lifetime. If the DataSource is unset, or if INFORMATION_SCHEMA
 * returns nothing for the table (e.g. permissions, table dropped
 * mid-stream), the adapter falls back to indexed names (`col0`,
 * `col1`, …) AND emits a WARN log + bumps the
 * `cdc.outbox.source.binlog.column_resolution.fallbacks` counter so
 * the operator sees the degradation.
 *
 * Checkpoint format: `<binlog filename>:<position>`. **The
 * `lastAckedCheckpoint` is held in memory only — it is NOT persisted
 * across restarts.** On restart the binlog client reconnects from the
 * server's current position (or wherever its own state machine
 * decides), which means at-least-once is currently best-effort for
 * this adapter. Persisted resume lands in Wave 5.2.
 *
 * Single-threaded contract on `poll`/`ack`/`close` per [RowChangeSource]
 * KDoc still applies — the binlog-client's internal thread is not the
 * orchestrator thread.
 */
@Suppress("LongParameterList")
class MySqlBinlogRowChangeSource(
    private val host: String,
    private val port: Int,
    private val username: String,
    private val password: String,
    private val serverId: Long = DEFAULT_SERVER_ID,
    /** Capacity of the internal buffer between binlog-thread and poller. */
    private val bufferSize: Int = DEFAULT_BUFFER_SIZE,
    /** Factory for the underlying client — overridable for tests. */
    private val clientFactory: (String, Int, String, String) -> BinaryLogClient =
        ::BinaryLogClient,
    /**
     * Optional JDBC handle pointing at the same MySQL instance the
     * binlog is being streamed from. When set, the adapter resolves
     * column names from `information_schema.columns`. When null, it
     * falls back to indexed `col0`, `col1`, … names.
     */
    private val dataSource: DataSource? = null,
    /**
     * Column-name lookup function — overridable for tests so we can
     * exercise the fallback path without a real MySQL. Returns the
     * column names in ordinal-position order, or `null` if the table
     * is unknown / inaccessible.
     */
    private val columnLookup: (DataSource, String, String) -> List<String>? = ::defaultColumnLookup,
    /** Metrics facade. Defaults to a no-op so non-Spring consumers stay quiet. */
    private val metrics: CdcOutboxMetrics = CdcOutboxMetrics.noop(),
) : RowChangeSource {

    private val opened = AtomicBoolean(false)
    private val client = AtomicReference<BinaryLogClient?>()

    /** Caches `tableId → "schema.table"` mappings from TABLE_MAP events. */
    private val tableCache = ConcurrentHashMap<Long, String>()

    /**
     * Cached column-name resolution keyed by `tableId`. A `null` value
     * means "lookup was attempted and failed" (so we don't retry on
     * every row and don't keep spamming WARN). An absent entry means
     * "lookup not yet attempted".
     */
    private val columnNamesByTableId = ConcurrentHashMap<Long, List<String>>()

    /** Active binlog filename — needed to build per-event checkpoints. */
    @Volatile
    private var currentBinlogFile: String = ""

    /**
     * Bounded buffer between the binlog client's internal thread and the
     * `poll` thread. `LinkedBlockingQueue` for back-pressure: if the
     * orchestrator can't keep up, the binlog client thread blocks on
     * `put`, which slows the upstream read.
     */
    private val buffer = LinkedBlockingQueue<RowChange>(bufferSize)

    /**
     * Last successfully-published checkpoint. In-memory only — see the
     * class KDoc; not used to resume on restart yet.
     */
    @Volatile
    private var lastAckedCheckpoint: String? = null

    override fun open() {
        if (!opened.compareAndSet(false, true)) {
            logger.debug("MySqlBinlogRowChangeSource already opened; ignoring duplicate open()")
            return
        }
        val c = clientFactory(host, port, username, password)
        c.serverId = serverId
        c.registerEventListener { event ->
            handleEvent(event)
        }
        client.set(c)
        // Connect asynchronously so this method returns once the client
        // is queued, matching the open() contract on RowChangeSource.
        Thread({
            try {
                c.connect()
            } catch (t: Throwable) {
                logger.error("MySqlBinlogRowChangeSource client died unexpectedly", t)
            }
        }, "cdc-outbox-mysql-binlog").apply { isDaemon = true }.start()
        logger.info("MySqlBinlogRowChangeSource connecting to {}:{} as serverId={}", host, port, serverId)
    }

    override fun poll(): RowChange? = buffer.poll()

    override fun ack(rowChange: RowChange) {
        lastAckedCheckpoint = rowChange.sourceCheckpoint
    }

    override fun close() {
        if (!opened.compareAndSet(true, false)) return
        runCatching { client.getAndSet(null)?.disconnect() }
        buffer.clear()
        tableCache.clear()
        columnNamesByTableId.clear()
        logger.info("MySqlBinlogRowChangeSource closed (last acked checkpoint: {})", lastAckedCheckpoint)
    }

    // TooGenericExceptionCaught: this catch is the last line of defence
    // for the binlog-client's listener thread. Anything that escapes here
    // would silently kill the listener and leave the queue dry forever.
    // We log and record a parse-error metric so the operator can act.
    @Suppress("TooGenericExceptionCaught")
    private fun handleEvent(event: com.github.shyiko.mysql.binlog.event.Event) {
        try {
            // `EventHeaderV4` is the post-5.0 binlog header — that's what
            // every supported MySQL release emits. The base `EventHeader`
            // doesn't expose `nextPosition`, which we need for the
            // per-event checkpoint.
            val header = event.getHeader<com.github.shyiko.mysql.binlog.event.EventHeaderV4>()
            when (header.eventType) {
                EventType.ROTATE -> {
                    val data = event.getData<RotateEventData>()
                    currentBinlogFile = data.binlogFilename
                }
                EventType.QUERY -> {
                    // QUERY events (BEGIN/COMMIT/DDL) are control plane — keep the
                    // binlog file moving but don't emit RowChanges.
                    event.getData<QueryEventData>()
                }
                EventType.TABLE_MAP -> {
                    val data = event.getData<TableMapEventData>()
                    tableCache[data.tableId] = "${data.database}.${data.table}"
                    resolveColumnNames(data.tableId, data.database, data.table)
                }
                EventType.EXT_WRITE_ROWS, EventType.WRITE_ROWS -> {
                    val data = event.getData<WriteRowsEventData>()
                    val table = tableCache[data.tableId] ?: return
                    val names = columnNamesByTableId[data.tableId]
                    data.rows.forEach { row ->
                        offer(
                            RowChange(
                                op = RowChange.Op.INSERT,
                                table = table,
                                sourceCheckpoint = "$currentBinlogFile:${header.nextPosition}",
                                occurredAt = Instant.ofEpochMilli(header.timestamp),
                                after = row.namedAsMap(names),
                            ),
                        )
                    }
                }
                EventType.EXT_UPDATE_ROWS, EventType.UPDATE_ROWS -> {
                    val data = event.getData<UpdateRowsEventData>()
                    val table = tableCache[data.tableId] ?: return
                    val names = columnNamesByTableId[data.tableId]
                    data.rows.forEach { entry ->
                        offer(
                            RowChange(
                                op = RowChange.Op.UPDATE,
                                table = table,
                                sourceCheckpoint = "$currentBinlogFile:${header.nextPosition}",
                                occurredAt = Instant.ofEpochMilli(header.timestamp),
                                before = entry.key.namedAsMap(names),
                                after = entry.value.namedAsMap(names),
                            ),
                        )
                    }
                }
                EventType.EXT_DELETE_ROWS, EventType.DELETE_ROWS -> {
                    val data = event.getData<DeleteRowsEventData>()
                    val table = tableCache[data.tableId] ?: return
                    val names = columnNamesByTableId[data.tableId]
                    data.rows.forEach { row ->
                        offer(
                            RowChange(
                                op = RowChange.Op.DELETE,
                                table = table,
                                sourceCheckpoint = "$currentBinlogFile:${header.nextPosition}",
                                occurredAt = Instant.ofEpochMilli(header.timestamp),
                                before = row.namedAsMap(names),
                            ),
                        )
                    }
                }
                else -> Unit  // ignore other event types
            }
        } catch (t: Throwable) {
            logger.warn("MySqlBinlogRowChangeSource failed to handle event {} ({})", event, t.javaClass.simpleName)
            metrics.recordBinlogParseError(t.javaClass.simpleName)
        }
    }

    /**
     * Resolves and caches column names for [tableId]. Called exactly
     * once on the first TABLE_MAP for the id; subsequent TABLE_MAPs
     * (binlog rotation re-emits them) hit the early-return. Falls back
     * to no caching when the DataSource is null or the lookup yields
     * nothing — each subsequent row event then triggers one fallback
     * metric per *row event*, but we WARN-log only at resolution time
     * (not per row) to avoid log spam.
     */
    private fun resolveColumnNames(tableId: Long, schema: String, table: String) {
        if (columnNamesByTableId.containsKey(tableId)) return
        val ds = dataSource
        if (ds == null) {
            logger.warn(
                "No DataSource configured for MySqlBinlogRowChangeSource; falling back to indexed column names " +
                    "for {}.{} (tableId={}).",
                schema, table, tableId,
            )
            metrics.recordBinlogColumnResolutionFallback("$schema.$table")
            return
        }
        val names = try {
            columnLookup(ds, schema, table)
        } catch (t: Exception) {
            logger.warn(
                "INFORMATION_SCHEMA lookup failed for {}.{} ({}); falling back to indexed column names.",
                schema, table, t.javaClass.simpleName, t,
            )
            metrics.recordBinlogColumnResolutionFallback("$schema.$table")
            return
        }
        if (names.isNullOrEmpty()) {
            logger.warn(
                "INFORMATION_SCHEMA returned no columns for {}.{}; falling back to indexed column names.",
                schema, table,
            )
            metrics.recordBinlogColumnResolutionFallback("$schema.$table")
            return
        }
        columnNamesByTableId[tableId] = names
        logger.debug(
            "Resolved {} columns for {}.{} (tableId={}) via INFORMATION_SCHEMA: {}",
            names.size, schema, table, tableId, names,
        )
    }

    private fun offer(change: RowChange) {
        // Blocks if buffer is full — back-pressure to the binlog client.
        buffer.put(change)
    }

    /**
     * Projects a binlog row's positional `Array<Serializable?>` onto a
     * column-name map. Uses the resolved [names] when available;
     * otherwise falls back to `col0`/`col1`/… and bumps the fallback
     * counter so the operator can detect missing INFORMATION_SCHEMA
     * coverage in production.
     */
    private fun Array<out java.io.Serializable?>.namedAsMap(names: List<String>?): Map<String, Any?> {
        if (names.isNullOrEmpty()) {
            return withIndex().associate { (i, value) -> "col$i" to value }
        }
        return withIndex().associate { (i, value) ->
            val name = names.getOrNull(i) ?: "col$i"
            name to value
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MySqlBinlogRowChangeSource::class.java)
        const val DEFAULT_SERVER_ID = 65_536L
        const val DEFAULT_BUFFER_SIZE = 1_024

        private const val COLUMN_LOOKUP_SQL =
            "SELECT column_name FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? " +
                "ORDER BY ordinal_position"

        /**
         * Default column-name lookup against `information_schema.columns`.
         * Returns `null` if the table is unknown, an empty list if the
         * table exists but exposes no columns to the connecting user
         * (effectively the same fallback path), or the ordered list of
         * names on success.
         */
        internal fun defaultColumnLookup(
            dataSource: DataSource,
            schema: String,
            table: String,
        ): List<String>? = dataSource.connection.use { conn ->
            conn.prepareStatement(COLUMN_LOOKUP_SQL).use { stmt ->
                stmt.setString(1, schema)
                stmt.setString(2, table)
                stmt.executeQuery().use { rs ->
                    val names = mutableListOf<String>()
                    while (rs.next()) names += rs.getString(1)
                    if (names.isEmpty()) null else names
                }
            }
        }
    }
}
