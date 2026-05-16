package br.com.fltech.cdc.outbox.publisher.adapter.source.mysql

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.port.InMemoryCheckpointStore
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.Event
import com.github.shyiko.mysql.binlog.event.EventHeaderV4
import com.github.shyiko.mysql.binlog.event.EventType
import com.github.shyiko.mysql.binlog.event.TableMapEventData
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Lifecycle smoke tests for the MySQL binlog adapter.
 *
 * Full event-handling coverage requires a real MySQL replica
 * (Testcontainers MySQL with `binlog_format=ROW`) and is tracked as a
 * follow-up integration test on the Wave 5 roadmap row. These cases
 * exercise the parts that don't need a running broker:
 *  - open is idempotent and registers a listener,
 *  - poll returns null when no events are buffered,
 *  - ack updates the last-acked checkpoint marker without raising,
 *  - close shuts the underlying client down idempotently,
 *  - checkpoint store is consulted on open and persisted on ack
 *    (Wave 5.2),
 *  - column-name cache invalidates on column-count change
 *    (Wave 5.2 / Round-9 MINOR #3) without bumping the fallback
 *    counter (that's reserved for genuine resolution failures).
 */
class MySqlBinlogRowChangeSourceTest {

    private val client = mockk<BinaryLogClient>(relaxed = true)
    private val factory: (String, Int, String, String) -> BinaryLogClient = { _, _, _, _ -> client }

    private fun newSource(
        dataSource: DataSource? = null,
        columnLookup: (DataSource, String, String) -> List<String>? = { _, _, _ -> null },
        metrics: CdcOutboxMetrics = CdcOutboxMetrics.noop(),
        checkpointStore: br.com.fltech.cdc.outbox.publisher.core.port.CheckpointStore? = null,
    ) = MySqlBinlogRowChangeSource(
        host = "localhost",
        port = 3306,
        username = "repl",
        password = "secret",
        clientFactory = factory,
        dataSource = dataSource,
        columnLookup = columnLookup,
        metrics = metrics,
        checkpointStore = checkpointStore,
    )

    @Test
    fun `open is idempotent and configures the underlying client once`() {
        val src = newSource()
        src.open()
        src.open() // duplicate call — should be a no-op
        verify(atMost = 1) { client.registerEventListener(any()) }
        src.close()
    }

    @Test
    fun `poll returns null when no events have been buffered yet`() {
        val src = newSource()
        src.open()
        assertNull(src.poll())
        src.close()
    }

    @Test
    fun `ack tolerates being called with arbitrary RowChange instances`() {
        val src = newSource()
        src.open()
        // Any RowChange is acceptable — ack just records the checkpoint.
        src.ack(
            RowChange(
                op = RowChange.Op.INSERT,
                table = "x.y",
                sourceCheckpoint = "mysql-bin.000001:120",
                occurredAt = java.time.Instant.EPOCH,
                after = mapOf("col0" to 1),
            ),
        )
        src.close()
    }

    @Test
    fun `close disconnects the underlying client and is idempotent`() {
        every { client.disconnect() } just Runs
        val src = newSource()
        src.open()
        src.close()
        src.close() // second call: no-op
        verify(atMost = 1) { client.disconnect() }
    }

    @Test
    fun `defaultColumnLookup returns ordered column names from INFORMATION_SCHEMA`() {
        // Walks the JDBC stack (DataSource → Connection → PreparedStatement →
        // ResultSet) and confirms (a) the query is bound with the right
        // parameters and (b) the result is materialised in ordinal order.
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns true andThen true andThen true andThen false
        every { rs.getString(1) } returns "id" andThen "status" andThen "total_cents"

        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs

        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt

        val ds = mockk<DataSource>()
        every { ds.connection } returns conn

        val names = MySqlBinlogRowChangeSource.defaultColumnLookup(ds, "shop", "orders")

        assertNotNull(names)
        assertEquals(listOf("id", "status", "total_cents"), names)
        verify { stmt.setString(1, "shop") }
        verify { stmt.setString(2, "orders") }
    }

    @Test
    fun `defaultColumnLookup returns null when the table is unknown to INFORMATION_SCHEMA`() {
        // Empty result set → null so the adapter can take its fallback
        // branch and emit the fallback metric.
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns false
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        val ds = mockk<DataSource>()
        every { ds.connection } returns conn

        val names = MySqlBinlogRowChangeSource.defaultColumnLookup(ds, "shop", "ghosts")

        assertNull(names)
    }

    @Test
    fun `open seeds binlogFilename and binlogPosition from a persisted checkpoint`() {
        val store = InMemoryCheckpointStore(initial = mapOf("binlog:65536" to "mysql-bin.000007:4321"))
        val src = newSource(checkpointStore = store)
        src.open()

        // The setters are exposed on BinaryLogClient as setBinlogFilename /
        // setBinlogPosition; with a relaxed mock the verification confirms
        // resume seeded the client before connect() ran.
        verify { client.binlogFilename = "mysql-bin.000007" }
        verify { client.binlogPosition = 4321L }
        assertEquals(1, store.loadCount)
        src.close()
    }

    @Test
    fun `open without a persisted checkpoint leaves the binlog client at its default position`() {
        // No initial entry — load returns null and we do NOT call the
        // setter (so the binlog client starts at head).
        val store = InMemoryCheckpointStore()
        val src = newSource(checkpointStore = store)
        src.open()
        verify(exactly = 0) { client.binlogFilename = any() }
        verify(exactly = 0) { client.binlogPosition = any() }
        src.close()
    }

    @Test
    fun `ack persists the latest checkpoint to the store on every call`() {
        val store = InMemoryCheckpointStore()
        val src = newSource(checkpointStore = store)
        src.open()

        src.ack(
            RowChange(
                op = RowChange.Op.INSERT,
                table = "x.y",
                sourceCheckpoint = "mysql-bin.000001:120",
                occurredAt = java.time.Instant.EPOCH,
            ),
        )
        src.ack(
            RowChange(
                op = RowChange.Op.INSERT,
                table = "x.y",
                sourceCheckpoint = "mysql-bin.000001:240",
                occurredAt = java.time.Instant.EPOCH,
            ),
        )

        assertEquals(2, store.saveCount)
        assertEquals("mysql-bin.000001:240", store.snapshot()["binlog:65536"])
        src.close()
    }

    @Test
    fun `malformed persisted checkpoint is ignored and the source still opens`() {
        val store = InMemoryCheckpointStore(initial = mapOf("binlog:65536" to "garbage"))
        val src = newSource(checkpointStore = store)
        src.open()
        // The malformed checkpoint MUST NOT crash the open; we just don't
        // seed the client and let it start at head.
        verify(exactly = 0) { client.binlogFilename = any() }
        verify(exactly = 0) { client.binlogPosition = any() }
        src.close()
    }

    @Test
    fun `column count change between two TABLE_MAP events invalidates the cached names without bumping fallbacks`() {
        // Capture the listener so we can hand-feed it TABLE_MAP events.
        val listenerSlot = slot<BinaryLogClient.EventListener>()
        every { client.registerEventListener(capture(listenerSlot)) } just Runs

        val lookups = mutableListOf<Int>()
        val lookup: (DataSource, String, String) -> List<String>? = { _, _, _ ->
            lookups += 1
            // Each lookup returns a fresh, valid name list — the point is
            // the lookup runs again after the invalidation, not the names.
            if (lookups.size == 1) listOf("id", "status") else listOf("id", "status", "amount")
        }
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val src = newSource(
            dataSource = mockk(),
            columnLookup = lookup,
            metrics = metrics,
        )
        src.open()

        val listener = listenerSlot.captured
        listener.onEvent(tableMapEvent(tableId = 1L, schema = "shop", table = "orders", columnCount = 2))
        listener.onEvent(tableMapEvent(tableId = 1L, schema = "shop", table = "orders", columnCount = 3))

        // Two lookups: one for the initial TABLE_MAP, one after invalidation.
        assertEquals(2, lookups.size)
        // Invalidation MUST NOT bump the fallback counter — that counter is
        // reserved for genuine resolution failures.
        assertEquals(
            0.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_COLUMN_RESOLUTION_FALLBACKS,
                CdcOutboxMetrics.TAG_TABLE, "shop.orders",
            ).count(),
        )
        src.close()
    }

    @Test
    fun `column count stable across repeated TABLE_MAP events does not re-query INFORMATION_SCHEMA`() {
        val listenerSlot = slot<BinaryLogClient.EventListener>()
        every { client.registerEventListener(capture(listenerSlot)) } just Runs

        val lookups = mutableListOf<Int>()
        val lookup: (DataSource, String, String) -> List<String>? = { _, _, _ ->
            lookups += 1
            listOf("id", "status")
        }
        val src = newSource(
            dataSource = mockk(),
            columnLookup = lookup,
        )
        src.open()

        val listener = listenerSlot.captured
        listener.onEvent(tableMapEvent(tableId = 1L, schema = "shop", table = "orders", columnCount = 2))
        // Binlog rotation re-emits TABLE_MAP with the same column count;
        // the cache must remain valid and the lookup must NOT re-run.
        listener.onEvent(tableMapEvent(tableId = 1L, schema = "shop", table = "orders", columnCount = 2))
        listener.onEvent(tableMapEvent(tableId = 1L, schema = "shop", table = "orders", columnCount = 2))

        assertEquals(1, lookups.size)
        src.close()
    }

    @Test
    fun `column type change with same count bumps schema_drift counter and invalidates the name cache`() {
        // Same column COUNT (3) on both TABLE_MAPs, but the type
        // vector flips — that's the ALTER TABLE MODIFY COLUMN signal
        // the existing column-count guard misses.
        val listenerSlot = slot<BinaryLogClient.EventListener>()
        every { client.registerEventListener(capture(listenerSlot)) } just Runs

        val lookups = mutableListOf<Int>()
        val lookup: (DataSource, String, String) -> List<String>? = { _, _, _ ->
            lookups += 1
            listOf("id", "status", "amount")
        }
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val src = newSource(dataSource = mockk(), columnLookup = lookup, metrics = metrics)
        src.open()

        val listener = listenerSlot.captured
        // Type codes: 3=INT, 15=VARCHAR, 4=FLOAT
        listener.onEvent(
            tableMapEventWithTypes(
                tableId = 1L,
                schema = "shop",
                table = "orders",
                types = byteArrayOf(3, 15, 4),
            ),
        )
        // ALTER TABLE orders MODIFY id BIGINT — type code shifts 3→8 (LONGLONG)
        listener.onEvent(
            tableMapEventWithTypes(
                tableId = 1L,
                schema = "shop",
                table = "orders",
                types = byteArrayOf(8, 15, 4),
            ),
        )

        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_SCHEMA_DRIFT,
                CdcOutboxMetrics.TAG_TABLE, "shop.orders",
            ).count(),
        )
        // Cache was invalidated → second lookup ran.
        assertEquals(2, lookups.size)
        src.close()
    }

    @Test
    fun `repeated TABLE_MAP with same type vector does NOT bump schema_drift`() {
        // Binlog rotation re-emits TABLE_MAPs with the same shape;
        // drift counter must stay flat.
        val listenerSlot = slot<BinaryLogClient.EventListener>()
        every { client.registerEventListener(capture(listenerSlot)) } just Runs

        val lookups = mutableListOf<Int>()
        val lookup: (DataSource, String, String) -> List<String>? = { _, _, _ ->
            lookups += 1
            listOf("id", "status", "amount")
        }
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val src = newSource(dataSource = mockk(), columnLookup = lookup, metrics = metrics)
        src.open()

        val listener = listenerSlot.captured
        repeat(3) {
            listener.onEvent(
                tableMapEventWithTypes(
                    tableId = 1L,
                    schema = "shop",
                    table = "orders",
                    types = byteArrayOf(3, 15, 4),
                ),
            )
        }

        // No drift, no re-lookup.
        assertEquals(
            0.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_SCHEMA_DRIFT,
                CdcOutboxMetrics.TAG_TABLE, "shop.orders",
            ).count(),
        )
        assertEquals(1, lookups.size)
        src.close()
    }

    @Test
    fun `first TABLE_MAP establishes baseline and does NOT report drift`() {
        // Drift is only meaningful on the second-or-later TABLE_MAP
        // for the same tableId. First sighting establishes the
        // baseline silently.
        val listenerSlot = slot<BinaryLogClient.EventListener>()
        every { client.registerEventListener(capture(listenerSlot)) } just Runs

        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        val src = newSource(metrics = metrics)
        src.open()

        listenerSlot.captured.onEvent(
            tableMapEventWithTypes(
                tableId = 1L,
                schema = "shop",
                table = "orders",
                types = byteArrayOf(3, 15, 4),
            ),
        )

        assertEquals(
            0.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_SCHEMA_DRIFT,
                CdcOutboxMetrics.TAG_TABLE, "shop.orders",
            ).count(),
        )
        src.close()
    }

    private fun tableMapEvent(
        tableId: Long,
        schema: String,
        table: String,
        columnCount: Int,
    ): Event = tableMapEventWithTypes(
        tableId = tableId,
        schema = schema,
        table = table,
        types = ByteArray(columnCount),
    )

    private fun tableMapEventWithTypes(
        tableId: Long,
        schema: String,
        table: String,
        types: ByteArray,
    ): Event {
        val header = EventHeaderV4().apply {
            eventType = EventType.TABLE_MAP
            timestamp = 0L
            nextPosition = 0L
        }
        val data = TableMapEventData().apply {
            this.tableId = tableId
            this.database = schema
            this.table = table
            this.columnTypes = types
        }
        return Event(header, data)
    }
}
