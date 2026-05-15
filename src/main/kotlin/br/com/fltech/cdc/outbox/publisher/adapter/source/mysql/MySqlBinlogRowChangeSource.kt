package br.com.fltech.cdc.outbox.publisher.adapter.source.mysql

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource
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
 * Checkpoint format: `<binlog filename>:<position>`. `ack` updates
 * the source's stored checkpoint so a restart resumes at the right
 * position. NOTE: the binlog client is event-driven and runs on its
 * own thread; this class buffers events into a bounded queue that
 * `poll()` drains one at a time, in order.
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
) : RowChangeSource {

    private val opened = AtomicBoolean(false)
    private val client = AtomicReference<BinaryLogClient?>()

    /** Caches `tableId → "schema.table"` mappings from TABLE_MAP events. */
    private val tableCache = ConcurrentHashMap<Long, String>()

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
     * Last successfully-published checkpoint. `ack` updates this so a
     * restart can resume.
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
        logger.info("MySqlBinlogRowChangeSource closed (last acked checkpoint: {})", lastAckedCheckpoint)
    }

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
                }
                EventType.EXT_WRITE_ROWS, EventType.WRITE_ROWS -> {
                    val data = event.getData<WriteRowsEventData>()
                    val table = tableCache[data.tableId] ?: return
                    data.rows.forEach { row ->
                        offer(
                            RowChange(
                                op = RowChange.Op.INSERT,
                                table = table,
                                sourceCheckpoint = "$currentBinlogFile:${header.nextPosition}",
                                occurredAt = Instant.ofEpochMilli(header.timestamp),
                                after = row.indexedAsMap(),
                            ),
                        )
                    }
                }
                EventType.EXT_UPDATE_ROWS, EventType.UPDATE_ROWS -> {
                    val data = event.getData<UpdateRowsEventData>()
                    val table = tableCache[data.tableId] ?: return
                    data.rows.forEach { entry ->
                        offer(
                            RowChange(
                                op = RowChange.Op.UPDATE,
                                table = table,
                                sourceCheckpoint = "$currentBinlogFile:${header.nextPosition}",
                                occurredAt = Instant.ofEpochMilli(header.timestamp),
                                before = entry.key.indexedAsMap(),
                                after = entry.value.indexedAsMap(),
                            ),
                        )
                    }
                }
                EventType.EXT_DELETE_ROWS, EventType.DELETE_ROWS -> {
                    val data = event.getData<DeleteRowsEventData>()
                    val table = tableCache[data.tableId] ?: return
                    data.rows.forEach { row ->
                        offer(
                            RowChange(
                                op = RowChange.Op.DELETE,
                                table = table,
                                sourceCheckpoint = "$currentBinlogFile:${header.nextPosition}",
                                occurredAt = Instant.ofEpochMilli(header.timestamp),
                                before = row.indexedAsMap(),
                            ),
                        )
                    }
                }
                else -> Unit  // ignore other event types
            }
        } catch (t: Throwable) {
            logger.warn("MySqlBinlogRowChangeSource failed to handle event {} ({})", event, t.javaClass.simpleName)
        }
    }

    private fun offer(change: RowChange) {
        // Blocks if buffer is full — back-pressure to the binlog client.
        buffer.put(change)
    }

    /**
     * The binlog client surfaces rows as `Array<Serializable?>` keyed
     * by column index (because column-name resolution depends on a
     * TABLE_MAP event being present). When `binlog_row_metadata=FULL`
     * is enabled the names are recoverable from the table map; this
     * library version exposes them only by index unless the caller
     * cross-references INFORMATION_SCHEMA. Until Wave 5.1 wires the
     * lookup, we expose columns as `col0`, `col1`, … and rely on the
     * MappingRules' include/rename to project them onto real names.
     */
    private fun Array<out java.io.Serializable?>.indexedAsMap(): Map<String, Any?> =
        withIndex().associate { (i, value) -> "col$i" to value }

    companion object {
        private val logger = LoggerFactory.getLogger(MySqlBinlogRowChangeSource::class.java)
        const val DEFAULT_SERVER_ID = 65_536L
        const val DEFAULT_BUFFER_SIZE = 1_024
    }
}
