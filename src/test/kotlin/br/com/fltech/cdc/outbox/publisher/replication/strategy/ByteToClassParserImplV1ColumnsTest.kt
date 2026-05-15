package br.com.fltech.cdc.outbox.publisher.replication.strategy

import br.com.fltech.cdc.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.cdc.outbox.publisher.replication.model.DeleteChange
import br.com.fltech.cdc.outbox.publisher.replication.model.InsertChange
import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import br.com.fltech.cdc.outbox.publisher.replication.model.UpdateChange
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/**
 * Parity coverage for [ByteToClassParserImplV1] mirroring the V2
 * column-surfacing assertions in [ByteToClassParserImplV2LsnTest]. The
 * V1 wire shape (`columnnames` / `columntypes` / `columnvalues`
 * parallel arrays plus a nested `oldkeys` wrapper) is translated into
 * the canonical
 * [br.com.fltech.cdc.outbox.publisher.replication.model.InsertChange] /
 * [br.com.fltech.cdc.outbox.publisher.replication.model.UpdateChange] /
 * [br.com.fltech.cdc.outbox.publisher.replication.model.DeleteChange]
 * shape so downstream consumers (notably
 * [br.com.fltech.cdc.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource])
 * see the same in-memory representation regardless of plugin version.
 */
class ByteToClassParserImplV1ColumnsTest {

    private val parser = ByteToClassParserImplV1(defaultMapper)

    @Test
    fun `INSERT surfaces columns from parallel arrays and leaves identity null`() {
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8198","change":[
              {"kind":"insert","schema":"public","table":"orders",
               "columnnames":["id","status","total_cents"],
               "columntypes":["integer","text","bigint"],
               "columnvalues":[42,"PENDING",999]}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as InsertChange

        assertEquals("insert", change.kind)
        assertEquals("public", change.schema)
        assertEquals("orders", change.table)
        assertEquals("0/16E8198", change.lsn)
        val columns = assertNotNull(change.columns)
        assertEquals(listOf("id", "status", "total_cents"), columns.map { it.name })
        assertEquals(listOf("integer", "text", "bigint"), columns.map { it.type })
        assertEquals(listOf<Any?>(42, "PENDING", 999), columns.map { it.value })
    }

    @Test
    fun `UPDATE surfaces both columns (post-image) and identity (oldkeys)`() {
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8200","change":[
              {"kind":"update","schema":"public","table":"orders",
               "columnnames":["id","status"],
               "columntypes":["integer","text"],
               "columnvalues":[42,"PAID"],
               "oldkeys":{
                 "keynames":["id"],
                 "keytypes":["integer"],
                 "keyvalues":[42]
               }}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as UpdateChange

        assertEquals("update", change.kind)
        assertEquals("public", change.schema)
        assertEquals("orders", change.table)
        assertEquals("0/16E8200", change.lsn)

        val columns = assertNotNull(change.columns)
        assertEquals(listOf("id", "status"), columns.map { it.name })
        assertEquals(listOf<Any?>(42, "PAID"), columns.map { it.value })

        val identity = assertNotNull(change.identity)
        assertEquals(listOf("id"), identity.map { it.name })
        assertEquals(listOf("integer"), identity.map { it.type })
        assertEquals(listOf<Any?>(42), identity.map { it.value })
    }

    @Test
    fun `DELETE leaves columns null and surfaces identity from oldkeys`() {
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8300","change":[
              {"kind":"delete","schema":"public","table":"orders",
               "oldkeys":{
                 "keynames":["id"],
                 "keytypes":["integer"],
                 "keyvalues":[42]
               }}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as DeleteChange

        assertEquals("delete", change.kind)
        assertEquals("public", change.schema)
        assertEquals("orders", change.table)
        assertEquals("0/16E8300", change.lsn)
        // DeleteChange carries no `columns` field by design — deletes
        // have no post-image. Identity is the only payload we expect.

        val identity = assertNotNull(change.identity)
        assertEquals(listOf("id"), identity.map { it.name })
        assertEquals(listOf<Any?>(42), identity.map { it.value })
    }

    @Test
    fun `wrapper nextlsn is propagated to every child change in the batch`() {
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8400","change":[
              {"kind":"insert","schema":"public","table":"orders",
               "columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]},
              {"kind":"update","schema":"public","table":"orders",
               "columnnames":["id"],"columntypes":["integer"],"columnvalues":[1],
               "oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},
              {"kind":"delete","schema":"public","table":"orders",
               "oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}}
            ]}
        """.trimIndent().toByteBuffer()

        val changes = parser.parse(payload)

        assertEquals(3, changes.size)
        assertEquals("0/16E8400", (changes[0] as InsertChange).lsn)
        assertEquals("0/16E8400", (changes[1] as UpdateChange).lsn)
        assertEquals("0/16E8400", (changes[2] as DeleteChange).lsn)
    }

    @Test
    fun `wrapper without nextlsn leaves child lsn null so the producer can fall back`() {
        val payload = """
            {"xid":12345,"change":[
              {"kind":"insert","schema":"public","table":"orders",
               "columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as InsertChange

        assertNull(change.lsn)
    }

    @Test
    fun `record missing columnnames-columnvalues degrades to null columns without throwing`() {
        // Mirrors a wal2json V1 `truncate` (or any column-less variant)
        // arriving in the same wrapper as legitimate row records — we
        // MUST keep draining instead of head-of-line blocking the
        // whole transaction.
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8500","change":[
              {"kind":"truncate","schema":"public","table":"orders"},
              {"kind":"insert","schema":"public","table":"orders",
               "columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}
            ]}
        """.trimIndent().toByteBuffer()

        val changes = parser.parse(payload)

        assertEquals(2, changes.size)
        val truncate = changes[0]
        // truncate isn't insert/update/delete — degrades to a generic Change
        assertEquals("truncate", truncate.kind)
        assertTrue(truncate !is InsertChange && truncate !is UpdateChange && truncate !is DeleteChange)
        // legitimate row still parses
        val insert = changes[1] as InsertChange
        assertNotNull(insert.columns)
    }

    @Test
    fun `UPDATE without oldkeys degrades to null identity without throwing`() {
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8600","change":[
              {"kind":"update","schema":"public","table":"orders",
               "columnnames":["id","status"],
               "columntypes":["integer","text"],
               "columnvalues":[42,"PAID"]}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as UpdateChange

        assertNotNull(change.columns)
        assertNull(change.identity)
    }

    @Test
    fun `mismatched columnnames vs columnvalues lengths degrade to null columns and keep draining`() {
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8700","change":[
              {"kind":"insert","schema":"public","table":"orders",
               "columnnames":["id","status"],
               "columntypes":["integer","text"],
               "columnvalues":[42]}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as InsertChange

        assertEquals("public", change.schema)
        assertEquals("orders", change.table)
        assertNull(change.columns)
    }

    @Test
    fun `null columnvalues entries are preserved verbatim`() {
        // wal2json V1 emits JSON `null` for NULL column values; the
        // parser must not coalesce them to an empty string or drop the
        // column entirely.
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8800","change":[
              {"kind":"insert","schema":"public","table":"orders",
               "columnnames":["id","notes"],
               "columntypes":["integer","text"],
               "columnvalues":[42,null]}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as InsertChange
        val columns = assertNotNull(change.columns)

        assertEquals(2, columns.size)
        assertEquals("notes", columns[1].name)
        assertNull(columns[1].value)
    }

    @Test
    fun `pg_logical_emit_message records translate to MessageChange with wrapper lsn`() {
        // Regression coverage: prior to this round the V1 parser
        // relied on Jackson's polymorphic dispatch on `Change` to
        // surface a MessageChange. With the explicit V1RowRecord
        // translation step we must keep producing a MessageChange
        // so SlotReaderMessageProducer's `change as MessageChange`
        // cast keeps working.
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8900","change":[
              {"kind":"message","transactional":true,
               "prefix":"orders.events","content":"{\"event\":1}"}
            ]}
        """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as MessageChange

        assertEquals("message", change.kind)
        assertEquals("orders.events", change.prefix)
        assertEquals("{\"event\":1}", change.content)
        assertTrue(change.transactional)
        assertEquals("0/16E8900", change.lsn)
    }

    @Test
    fun `message record missing required fields degrades to generic Change without throwing`() {
        // wal2json builds without `pg_logical_emit_message` support
        // could in theory emit a partial `message` record; the parser
        // must not blow up the whole batch.
        val payload = """
            {"xid":12345,"nextlsn":"0/16E8A00","change":[
              {"kind":"message","prefix":"orders.events"},
              {"kind":"insert","schema":"public","table":"orders",
               "columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}
            ]}
        """.trimIndent().toByteBuffer()

        val changes = parser.parse(payload)

        assertEquals(2, changes.size)
        assertEquals("message", changes[0].kind)
        assertTrue(changes[0] !is MessageChange)
        assertTrue(changes[1] is InsertChange)
    }

    private fun String.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(toByteArray(Charsets.UTF_8))
}
