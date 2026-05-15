package br.com.fltech.cdc.outbox.publisher.adapter.source.mysql

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.port.CheckpointStore
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
 * Checkpoint format: `<binlog filename>:<position>`. When a
 * [CheckpointStore] is wired (constructor arg `checkpointStore`) the
 * adapter:
 *  - on `open()`, calls `checkpointStore.load("binlog:<serverId>")`
 *    and (if a value is present) drives the underlying
 *    `BinaryLogClient` to resume from that file/position BEFORE
 *    `connect()` — this is what restores at-least-once across
 *    restarts.
 *  - on `ack(rowChange)`, persists the new checkpoint via
 *    `checkpointStore.save(...)`. The save runs on the orchestrator
 *    thread per the single-threaded contract. The default
 *    file-backed store performs an atomic `fsync + rename` per call,
 *    which is cheap enough that we don't debounce — operators value
 *    zero-loss over a marginal throughput win and per-message
 *    persistence sidesteps the "what if we crash between two saves"
 *    cliff.
 *  - keeps the in-memory `lastAckedCheckpoint` as a read-side cache
 *    so `close()` can log the high-water mark without touching disk.
 *
 * When the store is null the adapter degrades to the pre-Wave-5.2
 * behaviour: checkpoint lives in memory only and restart re-reads
 * from wherever the binlog client decides.
 *
 * Column-cache invalidation on schema change: when a `TABLE_MAP`
 * arrives for an already-known `tableId` with a different
 * `columnCount` than the cached resolution, the cached name list is
 * dropped and re-resolved via INFORMATION_SCHEMA on the next lookup.
 * Invalidation itself does NOT bump the
 * `column_resolution.fallbacks` counter — that one fires only when
 * the resolution actually FAILS (DataSource null, lookup throws, or
 * INFORMATION_SCHEMA returns nothing). Restating: invalidation =
 * "best-effort refresh"; fallback metric = "operator should know we
 * lost named columns".
 *
 * Single-threaded contract on `poll`/`ack`/`close` per [RowChangeSource]
 * KDoc still applies — the binlog-client's internal thread is not the
 * orchestrator thread.
 */
// LongParameterList: the binlog client needs JDBC handle + checkpoint
// store + metrics + the host/port/credentials surface; collapsing into
// a config object would scatter the wiring across files.
// TooManyFunctions: one method per binlog event type (TABLE_MAP,
// WRITE_ROWS, UPDATE_ROWS, DELETE_ROWS, QUERY) plus the lifecycle
// methods is intentionally on the class — splitting hurts
// discoverability for adapter readers.
@Suppress("LongParameterList", "TooManyFunctions")
class MySqlBinlogRowChangeSource(
    private val host: String,
    private val port: Int,
    private val username: String,
    private val password: String,
    /**
     * MySQL replication `server_id` used both as the binlog
     * client's identity and as the suffix of the checkpoint key
     * (`binlog:<serverId>`). Exposed (not `private`) so collaborators
     * — most notably the lag-probe adapter — can derive the
     * matching checkpoint key without a second source of truth.
     */
    val serverId: Long = DEFAULT_SERVER_ID,
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
    /**
     * Optional persistent checkpoint store. When wired the adapter
     * resumes from the previously-acked position on `open()` and
     * persists the new position on every `ack()`. When `null` the
     * adapter falls back to the legacy in-memory-only behaviour —
     * useful for tests and for consumers who accept rewind on
     * restart.
     */
    private val checkpointStore: CheckpointStore? = null,
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

    /**
     * Column count last seen for each `tableId` via `TABLE_MAP`. Used
     * to detect `ALTER TABLE`-style schema changes — when the
     * incoming count differs from the cached one we drop the
     * resolved name list so the next access re-queries
     * INFORMATION_SCHEMA. Without this, an `ALTER TABLE … ADD
     * COLUMN` between restarts would leave the adapter projecting
     * post-DDL row arrays onto pre-DDL column names.
     */
    private val columnCountByTableId = ConcurrentHashMap<Long, Int>()

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

    // TooGenericExceptionCaught: the connect()-thread catch is the
    // last line of defence — anything that escapes would silently
    // kill the daemon and leave the buffer dry forever. We log at
    // ERROR so the operator notices, but never re-throw.
    @Suppress("TooGenericExceptionCaught")
    override fun open() {
        if (!opened.compareAndSet(false, true)) {
            logger.debug("MySqlBinlogRowChangeSource already opened; ignoring duplicate open()")
            return
        }
        val c = clientFactory(host, port, username, password)
        c.serverId = serverId
        resumeFromCheckpoint(c)
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
        // Persist on every ack — at-least-once trumps throughput here.
        // The orchestrator is single-threaded over ack/poll so this
        // serialises naturally with the next read.
        checkpointStore?.let { store ->
            try {
                store.save(checkpointKey(), rowChange.sourceCheckpoint)
            } catch (e: Exception) {
                // A failed persist still leaves the in-memory checkpoint
                // valid for the current process lifetime — degrade
                // loudly but don't take the loop down.
                logger.warn(
                    "CheckpointStore.save failed for key={} value={} ({}); restart will rewind.",
                    checkpointKey(), rowChange.sourceCheckpoint, e.javaClass.simpleName, e,
                )
            }
        }
    }

    /**
     * Reads the previously-persisted checkpoint (if any) and seeds the
     * underlying [BinaryLogClient] so it resumes from that file/position
     * rather than the server's current head. No-op when no store is
     * wired, when the store is empty, or when the persisted value
     * cannot be parsed back into `<filename>:<position>`.
     */
    // ReturnCount: guard-clause sequence (no store → no persisted →
    // load failed → unparseable). Flattening to nested ifs hurts
    // readability; each guard is a distinct outcome.
    @Suppress("ReturnCount")
    private fun resumeFromCheckpoint(c: BinaryLogClient) {
        val store = checkpointStore ?: return
        val persisted = try {
            store.load(checkpointKey())
        } catch (e: Exception) {
            logger.warn(
                "CheckpointStore.load failed for key={} ({}); falling back to binlog head.",
                checkpointKey(), e.javaClass.simpleName, e,
            )
            return
        } ?: return
        val parsed = parseCheckpoint(persisted)
        if (parsed == null) {
            logger.warn(
                "Persisted checkpoint '{}' for key={} did not parse as <filename>:<position>; ignoring.",
                persisted, checkpointKey(),
            )
            return
        }
        val (file, position) = parsed
        c.binlogFilename = file
        c.binlogPosition = position
        currentBinlogFile = file
        lastAckedCheckpoint = persisted
        logger.info(
            "MySqlBinlogRowChangeSource resuming from persisted checkpoint key={} file={} position={}",
            checkpointKey(), file, position,
        )
    }

    private fun checkpointKey(): String = "binlog:$serverId"

    // ReturnCount: 3 distinct invalid shapes (missing colon, position
    // not numeric, well-formed) — multi-return is the natural form.
    @Suppress("ReturnCount")
    private fun parseCheckpoint(raw: String): Pair<String, Long>? {
        val idx = raw.lastIndexOf(':')
        if (idx <= 0 || idx == raw.length - 1) return null
        val file = raw.substring(0, idx)
        val position = raw.substring(idx + 1).toLongOrNull() ?: return null
        return file to position
    }

    override fun close() {
        if (!opened.compareAndSet(true, false)) return
        runCatching { client.getAndSet(null)?.disconnect() }
        buffer.clear()
        tableCache.clear()
        columnNamesByTableId.clear()
        columnCountByTableId.clear()
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
                    currentBinlogFile = event.getData<RotateEventData>().binlogFilename
                }
                EventType.QUERY -> {
                    // QUERY events (BEGIN/COMMIT/DDL) are control plane — keep the
                    // binlog file moving but don't emit RowChanges.
                    event.getData<QueryEventData>()
                }
                EventType.TABLE_MAP -> handleTableMap(event.getData())
                EventType.EXT_WRITE_ROWS, EventType.WRITE_ROWS -> handleWriteRows(event.getData(), header)
                EventType.EXT_UPDATE_ROWS, EventType.UPDATE_ROWS -> handleUpdateRows(event.getData(), header)
                EventType.EXT_DELETE_ROWS, EventType.DELETE_ROWS -> handleDeleteRows(event.getData(), header)
                else -> Unit  // ignore other event types
            }
        } catch (t: Throwable) {
            logger.warn("MySqlBinlogRowChangeSource failed to handle event {} ({})", event, t.javaClass.simpleName)
            metrics.recordBinlogParseError(t.javaClass.simpleName)
        }
    }

    private fun handleTableMap(data: TableMapEventData) {
        tableCache[data.tableId] = "${data.database}.${data.table}"
        invalidateOnColumnCountChange(data.tableId, data.columnTypes.size, data.database, data.table)
        resolveColumnNames(data.tableId, data.database, data.table)
    }

    private fun handleWriteRows(
        data: WriteRowsEventData,
        header: com.github.shyiko.mysql.binlog.event.EventHeaderV4,
    ) {
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

    private fun handleUpdateRows(
        data: UpdateRowsEventData,
        header: com.github.shyiko.mysql.binlog.event.EventHeaderV4,
    ) {
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

    private fun handleDeleteRows(
        data: DeleteRowsEventData,
        header: com.github.shyiko.mysql.binlog.event.EventHeaderV4,
    ) {
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

    /**
     * Drops the cached column-name list for [tableId] when the new
     * `TABLE_MAP` reports a different column count than the previous
     * one — the typical signal of an `ALTER TABLE … ADD/DROP COLUMN`
     * mid-stream. The next [resolveColumnNames] call will then refresh
     * the cache from INFORMATION_SCHEMA so subsequent row events
     * project onto the post-DDL schema.
     *
     * No metric is bumped here: invalidation is a best-effort refresh
     * path, not a failure. The fallback counter remains reserved for
     * genuine resolution failures.
     */
    private fun invalidateOnColumnCountChange(tableId: Long, newCount: Int, schema: String, table: String) {
        val previous = columnCountByTableId.put(tableId, newCount)
        if (previous != null && previous != newCount) {
            columnNamesByTableId.remove(tableId)
            logger.info(
                "MySqlBinlogRowChangeSource: column count for {}.{} (tableId={}) changed {} -> {}; " +
                    "invalidating cached column names.",
                schema, table, tableId, previous, newCount,
            )
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
    // ReturnCount: 4 distinct early-exits (already cached, no DataSource,
    // lookup raised, empty result). Each is a different operational
    // signal and emits a different log/metric; nesting would obscure them.
    @Suppress("ReturnCount")
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
                readColumnNames(stmt)
            }
        }

        private fun readColumnNames(stmt: java.sql.PreparedStatement): List<String>? =
            stmt.executeQuery().use { rs ->
                val names = mutableListOf<String>()
                while (rs.next()) names += rs.getString(1)
                if (names.isEmpty()) null else names
            }
    }
}
