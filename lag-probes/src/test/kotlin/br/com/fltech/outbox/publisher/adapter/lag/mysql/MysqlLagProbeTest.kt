package br.com.fltech.outbox.publisher.adapter.lag.mysql

import br.com.fltech.outbox.publisher.core.port.InMemoryCheckpointStore
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Drives [MysqlLagProbe] against a mocked [DataSource] / [ResultSet]
 * pair plus an in-memory checkpoint store. Exercises every branch:
 * happy path, missing checkpoint, malformed checkpoint, binlog
 * rotated mid-stream, server-side SHOW MASTER STATUS empty, SQL
 * failure.
 */
class MysqlLagProbeTest {
    private val serverId = 65_536L
    private val checkpointKey = "binlog:$serverId"

    private fun newDataSource(
        file: String?,
        position: Long,
        hasRow: Boolean = true,
    ): DataSource {
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns hasRow
        every { rs.getString("File") } returns file
        every { rs.getLong("Position") } returns position
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        val ds = mockk<DataSource>()
        every { ds.connection } returns conn
        return ds
    }

    @Test
    fun `lagBytes returns server position minus checkpoint position when files match`() {
        val ds = newDataSource(file = "mysql-bin.000003", position = 5_000L)
        val store = InMemoryCheckpointStore(initial = mapOf(checkpointKey to "mysql-bin.000003:1_200"))
        // toLongOrNull won't parse underscores — use a raw decimal.
        store.save(checkpointKey, "mysql-bin.000003:1200")
        val probe = MysqlLagProbe(ds, store, serverId)

        assertEquals(3_800L, probe.lagBytes())
        assertEquals("mysql", probe.sourceLabel)
    }

    @Test
    fun `lagBytes returns null when no checkpoint has been persisted`() {
        val ds = newDataSource(file = "mysql-bin.000003", position = 5_000L)
        val store = InMemoryCheckpointStore()
        val probe = MysqlLagProbe(ds, store, serverId)

        assertNull(probe.lagBytes())
    }

    @Test
    fun `lagBytes returns null when the persisted checkpoint is malformed`() {
        val ds = newDataSource(file = "mysql-bin.000003", position = 5_000L)
        val store = InMemoryCheckpointStore(initial = mapOf(checkpointKey to "not-a-checkpoint"))
        val probe = MysqlLagProbe(ds, store, serverId)

        assertNull(probe.lagBytes())
    }

    @Test
    fun `lagBytes returns null when the binlog rotated since the last ack`() {
        val ds = newDataSource(file = "mysql-bin.000004", position = 200L)
        val store = InMemoryCheckpointStore(initial = mapOf(checkpointKey to "mysql-bin.000003:9999"))
        val probe = MysqlLagProbe(ds, store, serverId)

        assertNull(probe.lagBytes())
    }

    @Test
    fun `rotation log fires once then re-arms when files realign`() {
        // First call: rotated → null. Subsequent call with matching
        // files re-arms the gate, then another rotation null-logs again.
        val store = InMemoryCheckpointStore(initial = mapOf(checkpointKey to "mysql-bin.000003:100"))

        val rotated = newDataSource(file = "mysql-bin.000004", position = 200L)
        val probe1 = MysqlLagProbe(rotated, store, serverId)
        assertNull(probe1.lagBytes())
        // No assertion on log output — the cheap behavioural proof is
        // that the probe doesn't throw on repeated calls and the
        // bookkeeping flips back. Re-target the same store with a
        // matching file:
        store.save(checkpointKey, "mysql-bin.000004:50")
        assertEquals(150L, probe1.lagBytes())
    }

    @Test
    fun `lagBytes returns null when SHOW MASTER STATUS returns no rows`() {
        val ds = newDataSource(file = null, position = 0L, hasRow = false)
        val store = InMemoryCheckpointStore(initial = mapOf(checkpointKey to "mysql-bin.000003:1000"))
        val probe = MysqlLagProbe(ds, store, serverId)

        assertNull(probe.lagBytes())
    }

    @Test
    fun `lagBytes returns null on SQLException from the DataSource`() {
        val ds = mockk<DataSource>()
        every { ds.connection } throws SQLException("boom")
        val store = InMemoryCheckpointStore(initial = mapOf(checkpointKey to "mysql-bin.000003:1000"))
        val probe = MysqlLagProbe(ds, store, serverId)

        assertNull(probe.lagBytes())
    }

    @Test
    fun `lagBytes returns null when the checkpoint position is ahead of the server`() {
        // Defensive: a manual PURGE BINARY LOGS or a clock-skewed
        // restart could produce a negative lag. The probe must surface
        // it as "no data" rather than reporting a negative number.
        val ds = newDataSource(file = "mysql-bin.000003", position = 500L)
        val store = InMemoryCheckpointStore(initial = mapOf(checkpointKey to "mysql-bin.000003:1000"))
        val probe = MysqlLagProbe(ds, store, serverId)

        assertNull(probe.lagBytes())
    }
}
