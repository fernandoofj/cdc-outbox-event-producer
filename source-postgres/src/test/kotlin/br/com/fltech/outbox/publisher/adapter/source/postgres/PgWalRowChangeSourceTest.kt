package br.com.fltech.outbox.publisher.adapter.source.postgres

import br.com.fltech.outbox.publisher.core.domain.RowChange
import br.com.fltech.outbox.publisher.core.port.InMemoryCheckpointStore
import br.com.fltech.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.outbox.publisher.replication.config.PostgresConfigurationMother
import br.com.fltech.outbox.publisher.replication.config.ReplicationConfigurationMother
import br.com.fltech.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.outbox.publisher.replication.connector.PostgresConnector
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.postgresql.replication.LogSequenceNumber
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Drives the Postgres row-level adapter end-to-end against a mocked
 * [PostgresConnector], skipping the actual replication-slot dance.
 * The point is to verify the wal2json `format-version=2`
 * I/U/D → [RowChange] translation and the `ack` → slot+store
 * fan-out.
 */
class PgWalRowChangeSourceTest {
    private val pgConfig = PostgresConfigurationMother.build()
    private val replicationConfig = ReplicationConfigurationMother.build()
    private val connectionProvider = mockk<ConnectionProvider>()

    private fun newSource(
        connector: PostgresConnector,
        checkpointStore: br.com.fltech.outbox.publisher.core.port.CheckpointStore? = null,
    ) = PgWalRowChangeSource(
        postgresConfiguration = pgConfig,
        replicationConfiguration = replicationConfig,
        connectionProvider = connectionProvider,
        objectMapper = defaultMapper,
        checkpointStore = checkpointStore,
        connectorFactory = { _, _, _ -> connector },
    )

    @Test
    fun `poll on an empty stream returns null`() {
        val connector = mockk<PostgresConnector>(relaxed = true)
        every { connector.readPending() } returns null
        val src = newSource(connector)
        src.open()
        assertNull(src.poll())
        src.close()
    }

    @Test
    fun `INSERT row emits a RowChange with after-image columns and lsn checkpoint`() {
        val payload =
            """
            {"xid":1,"action":"I","schema":"public","table":"orders","lsn":"0/16E8198",
             "columns":[{"name":"id","type":"integer","value":42},
                        {"name":"status","type":"text","value":"NEW"}]}
            """.trimIndent()
        val connector = mockk<PostgresConnector>(relaxed = true)
        every { connector.readPending() } returns payload.asBuffer() andThen null
        every { connector.lastReceivedLsn() } returns LogSequenceNumber.valueOf("0/16E0000")

        val src = newSource(connector)
        src.open()
        val row = src.poll()
        assertNotNull(row)
        assertEquals(RowChange.Op.INSERT, row.op)
        assertEquals("public.orders", row.table)
        assertEquals("0/16E8198", row.sourceCheckpoint)
        assertEquals(mapOf("id" to 42, "status" to "NEW"), row.after)
        assertEquals(emptyMap(), row.before)
        src.close()
    }

    @Test
    fun `UPDATE row emits before via identity and after via columns`() {
        val payload =
            """
            {"xid":1,"action":"U","schema":"public","table":"orders","lsn":"0/16E8200",
             "columns":[{"name":"id","value":42},{"name":"status","value":"PAID"}],
             "identity":[{"name":"id","value":42}]}
            """.trimIndent()
        val connector = mockk<PostgresConnector>(relaxed = true)
        every { connector.readPending() } returns payload.asBuffer() andThen null
        every { connector.lastReceivedLsn() } returns LogSequenceNumber.valueOf("0/16E0000")

        val src = newSource(connector)
        src.open()
        val row = src.poll()
        assertNotNull(row)
        assertEquals(RowChange.Op.UPDATE, row.op)
        assertEquals("public.orders", row.table)
        assertEquals("0/16E8200", row.sourceCheckpoint)
        assertEquals(mapOf("id" to 42), row.before)
        assertEquals(mapOf("id" to 42, "status" to "PAID"), row.after)
        src.close()
    }

    @Test
    fun `DELETE row emits before via identity and no after-image`() {
        val payload =
            """
            {"xid":1,"action":"D","schema":"public","table":"orders","lsn":"0/16E8300",
             "identity":[{"name":"id","value":42}]}
            """.trimIndent()
        val connector = mockk<PostgresConnector>(relaxed = true)
        every { connector.readPending() } returns payload.asBuffer() andThen null
        every { connector.lastReceivedLsn() } returns LogSequenceNumber.valueOf("0/16E0000")

        val src = newSource(connector)
        src.open()
        val row = src.poll()
        assertNotNull(row)
        assertEquals(RowChange.Op.DELETE, row.op)
        assertEquals("public.orders", row.table)
        assertEquals(mapOf("id" to 42), row.before)
        assertEquals(emptyMap(), row.after)
        src.close()
    }

    @Test
    fun `MessageChange records are ignored — the legacy adapter owns that flow`() {
        val payload =
            """
            {"xid":1,"action":"M","prefix":"sns://t","content":"{}","lsn":"0/16E8400"}
            """.trimIndent()
        val connector = mockk<PostgresConnector>(relaxed = true)
        // First poll consumes the message record (returns null since
        // we ignore it), second poll returns null genuinely.
        every { connector.readPending() } returns payload.asBuffer() andThen null
        every { connector.lastReceivedLsn() } returns LogSequenceNumber.valueOf("0/16E0000")

        val src = newSource(connector)
        src.open()
        assertNull(src.poll())
        src.close()
    }

    @Test
    fun `ack advances the slot LSN AND persists through the checkpoint store`() {
        val connector = mockk<PostgresConnector>(relaxed = true)
        every { connector.readPending() } returns null
        every { connector.setStreamLsn(any()) } just Runs

        val store = InMemoryCheckpointStore()
        val src = newSource(connector, checkpointStore = store)
        src.open()

        val row =
            RowChange(
                op = RowChange.Op.INSERT,
                table = "public.orders",
                sourceCheckpoint = "0/16E8198",
                occurredAt = java.time.Instant.EPOCH,
                after = emptyMap(),
            )
        src.ack(row)

        verify { connector.setStreamLsn(LogSequenceNumber.valueOf("0/16E8198")) }
        assertEquals("0/16E8198", store.snapshot()["pg-wal:slot-name"])
        src.close()
    }

    @Test
    fun `ack with INVALID_LSN does not call setStreamLsn but still persists locally`() {
        // Defensive: a malformed checkpoint in a RowChange shouldn't
        // crash the loop. Slot stays put; the store records the
        // payload for diagnostics.
        val connector = mockk<PostgresConnector>(relaxed = true)
        every { connector.readPending() } returns null
        val store = InMemoryCheckpointStore()
        val src = newSource(connector, checkpointStore = store)
        src.open()

        val row =
            RowChange(
                op = RowChange.Op.INSERT,
                table = "public.orders",
                // INVALID_LSN
                sourceCheckpoint = "0/0",
                occurredAt = java.time.Instant.EPOCH,
                after = emptyMap(),
            )
        src.ack(row)

        verify(exactly = 0) { connector.setStreamLsn(any()) }
        assertEquals("0/0", store.snapshot()["pg-wal:slot-name"])
        src.close()
    }

    private fun String.asBuffer(): ByteBuffer = ByteBuffer.wrap(toByteArray(Charsets.UTF_8))
}
