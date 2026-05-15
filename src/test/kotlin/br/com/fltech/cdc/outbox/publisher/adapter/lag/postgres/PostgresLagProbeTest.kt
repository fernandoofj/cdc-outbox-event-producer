package br.com.fltech.cdc.outbox.publisher.adapter.lag.postgres

import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfigurationMother
import br.com.fltech.cdc.outbox.publisher.replication.connector.ConnectionProvider
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Drives [PostgresLagProbe] against a mocked
 * [ConnectionProvider] so we exercise the SQL plumbing (statement
 * preparation, slot-name binding, result-set parsing) without
 * standing up Postgres.
 */
class PostgresLagProbeTest {
    private val pgConfig = PostgresConfigurationMother.build()

    @Test
    fun `lagBytes returns the pg_wal_lsn_diff value when the slot exists`() {
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns true
        every { rs.getLong(1) } returns 4_096L
        every { rs.wasNull() } returns false
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        val provider = mockk<ConnectionProvider>()
        every { provider.getConnection(any(), any()) } returns conn

        val probe = PostgresLagProbe(pgConfig, provider, slotName = "orders_outbox_slot")

        assertEquals(4_096L, probe.lagBytes())
        assertEquals("postgres", probe.sourceLabel)

        val slotName = slot<String>()
        verify { stmt.setString(1, capture(slotName)) }
        assertEquals("orders_outbox_slot", slotName.captured)
    }

    @Test
    fun `lagBytes returns null when the slot row is missing`() {
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns false
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        val provider = mockk<ConnectionProvider>()
        every { provider.getConnection(any(), any()) } returns conn

        val probe = PostgresLagProbe(pgConfig, provider, slotName = "missing_slot")

        assertNull(probe.lagBytes())
    }

    @Test
    fun `lagBytes returns null when confirmed_flush_lsn is SQL NULL`() {
        // pg_wal_lsn_diff(NULL, …) -> NULL → ResultSet.getLong returns 0
        // AND wasNull() returns true. The probe must distinguish that
        // from a legitimate zero-byte lag.
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns true
        every { rs.getLong(1) } returns 0L
        every { rs.wasNull() } returns true
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        val provider = mockk<ConnectionProvider>()
        every { provider.getConnection(any(), any()) } returns conn

        val probe = PostgresLagProbe(pgConfig, provider, slotName = "fresh_slot")

        assertNull(probe.lagBytes())
    }

    @Test
    fun `lagBytes returns null on SQLException`() {
        val provider = mockk<ConnectionProvider>()
        every { provider.getConnection(any(), any()) } throws SQLException("boom")

        val probe = PostgresLagProbe(pgConfig, provider, slotName = "any_slot")

        assertNull(probe.lagBytes())
    }

    @Test
    fun `lagBytes uses the query connection properties not the replication ones`() {
        // Replication-mode connections demand `replication=database` which
        // the lag probe must NOT request — pg_replication_slots is a
        // plain catalogue read on the query channel.
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns true
        every { rs.getLong(1) } returns 0L
        every { rs.wasNull() } returns false
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        val provider = mockk<ConnectionProvider>()
        val capturedProps = slot<Properties>()
        every { provider.getConnection(any(), capture(capturedProps)) } returns conn

        val probe = PostgresLagProbe(pgConfig, provider, slotName = "s")
        probe.lagBytes()

        // PGProperty.REPLICATION key is "replication" — must be absent.
        assertNull(capturedProps.captured.getProperty("replication"))
    }
}
