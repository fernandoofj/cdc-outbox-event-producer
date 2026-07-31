package br.com.fltech.outbox.publisher.replication.strategy

import br.com.fltech.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.outbox.publisher.replication.model.MessageChange
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Verifies that `wal2json` format-version 2 records carrying `include-lsn=true`
 * surface their per-message `lsn` on the parsed `MessageChange`. This is the
 * data point [br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer]
 * uses to advance the slot exactly, instead of falling back to the stream's
 * high-water mark.
 */
class ByteToClassParserImplV2LsnTest {
    private val parser = ByteToClassParserImplV2(defaultMapper)

    @Test
    fun `parses lsn when wal2json include-lsn is enabled`() {
        val payload =
            """
            {"xid":12345,"action":"M","prefix":"orders.events",
             "content":"{\"event\":1}","lsn":"0/16E8198"}
            """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single()

        assertEquals("message", change.kind)
        val messageChange = change as MessageChange
        assertEquals("orders.events", messageChange.prefix)
        assertEquals("0/16E8198", messageChange.lsn)
    }

    @Test
    fun `null lsn is tolerated when wal2json include-lsn is disabled`() {
        val payload =
            """
            {"xid":12345,"action":"M","prefix":"orders.events","content":"{\"event\":1}"}
            """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single() as MessageChange

        assertNull(change.lsn)
    }

    @Test
    fun `non-message records do not carry an lsn on the parsed Change`() {
        val payload =
            """
            {"xid":12345,"action":"I","lsn":"0/16E8198"}
            """.trimIndent().toByteBuffer()

        val change = parser.parse(payload).single()

        assertEquals("insert", change.kind)
    }

    private fun String.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(toByteArray(Charsets.UTF_8))
}
