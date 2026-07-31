package br.com.fltech.outbox.publisher.adapter.replay

import br.com.fltech.outbox.publisher.core.domain.RowChange
import br.com.fltech.outbox.publisher.core.port.RowChangeSource
import br.com.fltech.outbox.publisher.core.port.SourceReplayer
import br.com.fltech.outbox.publisher.core.port.UnsupportedReplayException
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData
import com.github.shyiko.mysql.binlog.event.EventHeaderV4
import com.github.shyiko.mysql.binlog.event.EventType
import com.github.shyiko.mysql.binlog.event.RotateEventData
import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

// LongParameterList: same rationale as the live MySqlBinlogRowChangeSource —
// host/port/credentials + serverId + JDBC handle for column resolution +
// factory hooks are all genuinely independent collaborators.

/**
 * Replays MySQL binlog row events from a specific file:position
 * window. Opens a separate [BinaryLogClient] with an isolated
 * `serverId` so the live producer's binlog session is never
 * disturbed. The bounded source emits row changes until the binlog
 * reaches the configured stop position, then `poll()` returns `null`.
 *
 * `ack()` is a no-op — replay must NOT advance any persisted
 * checkpoint; the live producer's state is read-only from this
 * adapter's perspective.
 *
 * Column-name resolution is reused from the live source's
 * `INFORMATION_SCHEMA` lookup pattern. The replayer takes its own
 * `DataSource` to keep concurrent usage safe.
 */
@Suppress("LongParameterList")
class MySqlBinlogReplayer(
    private val host: String,
    private val port: Int,
    private val username: String,
    private val password: String,
    /**
     * `serverId` for the binlog client. Operator must pick a value
     * NOT in use by any live source (the producer typically uses
     * the default `65_536`). The auto-config picks a value in the
     * `[1_000_000, 9_999_999]` range to keep replay clearly out of
     * the live range.
     */
    private val serverId: Long,
    private val dataSource: DataSource? = null,
    private val clientFactory: (String, Int, String, String) -> BinaryLogClient = ::BinaryLogClient,
    private val bufferSize: Int = DEFAULT_BUFFER_SIZE,
) : SourceReplayer {
    override val sourceKind: String = SOURCE_KIND

    override fun openBoundedSource(
        fromPosition: String,
        toPosition: String,
    ): RowChangeSource {
        val (fromFile, fromOffset) =
            parsePosition(fromPosition)
                ?: throw UnsupportedReplayException(
                    "fromPosition '$fromPosition' did not parse as <binlog-file>:<offset>",
                )
        val (toFile, toOffset) =
            parsePosition(toPosition)
                ?: throw UnsupportedReplayException(
                    "toPosition '$toPosition' did not parse as <binlog-file>:<offset>",
                )
        return BoundedMySqlBinlogSource(
            host = host,
            port = port,
            username = username,
            password = password,
            serverId = serverId,
            dataSource = dataSource,
            clientFactory = clientFactory,
            fromFile = fromFile,
            fromOffset = fromOffset,
            toFile = toFile,
            toOffset = toOffset,
            bufferSize = bufferSize,
        )
    }

    // ReturnCount: three guard branches map to three distinct invalid
    // shapes (missing colon, position not numeric, well-formed).
    @Suppress("ReturnCount")
    private fun parsePosition(raw: String): Pair<String, Long>? {
        val idx = raw.lastIndexOf(':')
        if (idx <= 0 || idx == raw.length - 1) return null
        val file = raw.substring(0, idx)
        val pos = raw.substring(idx + 1).toLongOrNull() ?: return null
        return file to pos
    }

    companion object {
        const val SOURCE_KIND = "mysql-binlog"
        const val DEFAULT_BUFFER_SIZE = 1_024
    }
}

// LongParameterList + TooManyFunctions: 12 ctor params are the
// binlog client surface (host/port/creds/serverId), the from/to
// window, and the factory hooks; one method per binlog event type
// matches the live source's pattern.

/**
 * Bounded [RowChangeSource] backed by a single binlog session.
 * `poll()` returns null once the binlog cursor crosses the stop
 * position OR the binlog drains earlier.
 */
@Suppress("LongParameterList", "TooManyFunctions")
internal class BoundedMySqlBinlogSource(
    private val host: String,
    private val port: Int,
    private val username: String,
    private val password: String,
    private val serverId: Long,
    private val dataSource: DataSource?,
    private val clientFactory: (String, Int, String, String) -> BinaryLogClient,
    private val fromFile: String,
    private val fromOffset: Long,
    private val toFile: String,
    private val toOffset: Long,
    bufferSize: Int,
) : RowChangeSource {
    private val opened = AtomicBoolean(false)
    private val finished = AtomicBoolean(false)
    private val client = AtomicReference<BinaryLogClient?>()
    private val buffer = LinkedBlockingQueue<RowChange>(bufferSize)
    private val tableCache = ConcurrentHashMap<Long, String>()
    private val columnNamesByTableId = ConcurrentHashMap<Long, List<String>>()
    private var currentBinlogFile: String = fromFile

    override fun open() {
        if (!opened.compareAndSet(false, true)) return
        val c = clientFactory(host, port, username, password)
        c.serverId = serverId
        c.binlogFilename = fromFile
        c.binlogPosition = fromOffset
        c.registerEventListener { event -> handleEvent(event) }
        client.set(c)
        // Thread carries an UncaughtExceptionHandler so an Error
        // (OOM, etc) marks the session finished and logs ERROR
        // — Exception is caught in-method to mark finished too,
        // both paths surface to the operator without silent death.
        Thread({
            try {
                c.connect()
            } catch (e: Exception) {
                logger.error("MySqlBinlogReplayer client died unexpectedly", e)
                finished.set(true)
            }
        }, "cdc-outbox-replay-mysql-$serverId").apply {
            isDaemon = true
            uncaughtExceptionHandler =
                Thread.UncaughtExceptionHandler { _, t ->
                    logger.error("MySqlBinlogReplayer daemon died with unrecoverable error", t)
                    finished.set(true)
                }
        }.start()
        logger.info(
            "MySqlBinlogReplayer: started replay session serverId={} from={}:{} to={}:{}",
            serverId,
            fromFile,
            fromOffset,
            toFile,
            toOffset,
        )
    }

    override fun poll(): RowChange? {
        if (finished.get() && buffer.isEmpty()) return null
        return buffer.poll(POLL_WAIT_MS, TimeUnit.MILLISECONDS)
    }

    /** Replay must NOT advance any production checkpoint. */
    override fun ack(rowChange: RowChange) = Unit

    override fun close() {
        if (!opened.compareAndSet(true, false)) return
        runCatching { client.getAndSet(null)?.disconnect() }
        buffer.clear()
        tableCache.clear()
        columnNamesByTableId.clear()
    }

    // Catch Exception only — Errors escape to the daemon thread's
    // UncaughtExceptionHandler (set in `open()`) so the operator
    // sees them without us silently swallowing OOM/StackOverflow.
    private fun handleEvent(event: com.github.shyiko.mysql.binlog.event.Event) {
        if (finished.get()) return
        try {
            val header = event.getHeader<EventHeaderV4>()
            if (reachedStop(header.nextPosition)) {
                logger.info(
                    "MySqlBinlogReplayer: stop position {}:{} reached; draining session",
                    currentBinlogFile,
                    header.nextPosition,
                )
                finished.set(true)
                runCatching { client.get()?.disconnect() }
                return
            }
            when (header.eventType) {
                EventType.ROTATE -> currentBinlogFile = event.getData<RotateEventData>().binlogFilename
                EventType.TABLE_MAP -> handleTableMap(event.getData())
                EventType.EXT_WRITE_ROWS, EventType.WRITE_ROWS -> handleWriteRows(event.getData(), header)
                EventType.EXT_UPDATE_ROWS, EventType.UPDATE_ROWS -> handleUpdateRows(event.getData(), header)
                EventType.EXT_DELETE_ROWS, EventType.DELETE_ROWS -> handleDeleteRows(event.getData(), header)
                else -> Unit
            }
        } catch (e: Exception) {
            logger.warn("MySqlBinlogReplayer: dropped event {} ({})", event, e.javaClass.simpleName, e)
        }
    }

    private fun reachedStop(nextPosition: Long): Boolean {
        val fileCmp = compareBinlogFiles(currentBinlogFile, toFile)
        return when {
            fileCmp < 0 -> false // still in earlier binlog file
            fileCmp > 0 -> true // we already passed the target file
            else -> nextPosition >= toOffset
        }
    }

    /**
     * Compares two binlog filenames lexicographically — works
     * because MySQL names them `mysql-bin.000001`, `000002`, … in
     * monotonically increasing sequence.
     */
    private fun compareBinlogFiles(
        a: String,
        b: String,
    ): Int = a.compareTo(b)

    private fun handleTableMap(data: TableMapEventData) {
        tableCache[data.tableId] = "${data.database}.${data.table}"
        resolveColumnNames(data.tableId, data.database, data.table)
    }

    // ReturnCount: 3 distinct early exits (already cached, no
    // DataSource, lookup failed). Same guard-clause rationale.
    @Suppress("ReturnCount")
    private fun resolveColumnNames(
        tableId: Long,
        schema: String,
        table: String,
    ) {
        if (columnNamesByTableId.containsKey(tableId)) return
        val ds = dataSource ?: return
        val names =
            try {
                ds.connection.use { conn ->
                    conn.prepareStatement(COLUMN_LOOKUP_SQL).use { stmt ->
                        stmt.setString(1, schema)
                        stmt.setString(2, table)
                        readNames(stmt)
                    }
                }
            } catch (e: Exception) {
                logger.warn(
                    "MySqlBinlogReplayer: column lookup failed for {}.{} ({}); falling back to indexed names",
                    schema,
                    table,
                    e.javaClass.simpleName,
                    e,
                )
                return
            }
        if (!names.isNullOrEmpty()) columnNamesByTableId[tableId] = names
    }

    private fun readNames(stmt: PreparedStatement): List<String>? =
        stmt.executeQuery().use { rs ->
            val names = mutableListOf<String>()
            while (rs.next()) names += rs.getString(1)
            if (names.isEmpty()) null else names
        }

    private fun handleWriteRows(
        data: WriteRowsEventData,
        header: EventHeaderV4,
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
        header: EventHeaderV4,
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
        header: EventHeaderV4,
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

    private fun offer(change: RowChange) {
        buffer.put(change)
    }

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
        private val logger = LoggerFactory.getLogger(BoundedMySqlBinlogSource::class.java)
        private const val POLL_WAIT_MS = 100L
        private const val COLUMN_LOOKUP_SQL =
            "SELECT column_name FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? " +
                "ORDER BY ordinal_position"
    }
}
