package br.com.fltech.cdc.outbox.publisher.replication.strategy

import br.com.fltech.cdc.outbox.publisher.replication.model.Change
import br.com.fltech.cdc.outbox.publisher.replication.model.DeleteChange
import br.com.fltech.cdc.outbox.publisher.replication.model.InsertChange
import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import br.com.fltech.cdc.outbox.publisher.replication.model.SlotMessageV2
import br.com.fltech.cdc.outbox.publisher.replication.model.UpdateChange
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.ByteBuffer
import org.springframework.stereotype.Component

/**
 * Decodes a wal2json `format-version=2` record. One record per
 * call. The resulting [Change] subtypes carry per-record data:
 *
 *  - [MessageChange] — `prefix`, `content`, `lsn` (the legacy
 *    `pg_logical_emit_message` path used by
 *    [br.com.fltech.cdc.outbox.publisher.adapter.source.postgres.PgLogicalReplicationCdcSource]).
 *  - [InsertChange] / [UpdateChange] / [DeleteChange] — `schema`,
 *    `table`, `columns` (post-image), `identity` (pre-image
 *    projection on the replica identity). These are the data points
 *    [br.com.fltech.cdc.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource]
 *    needs to emit fully-populated
 *    [br.com.fltech.cdc.outbox.publisher.core.domain.RowChange]s.
 */
@Component
class ByteToClassParserImplV2(
    private val defaultMapper: ObjectMapper
) : ByteToClassParser {
    override fun parse(byteBufferMessage: ByteBuffer): List<Change> {
        val byteArray = ByteArray(byteBufferMessage.remaining())
        byteBufferMessage.get(byteArray)
        val jsonString = String(byteArray, Charsets.UTF_8)
        val slotMessageV2 = defaultMapper.readValue(jsonString, SlotMessageV2::class.java)

        val change: Change = when {
            slotMessageV2.action == MESSAGE_TYPE_V2 -> MessageChange(
                kindInput = MESSAGE_TYPE_V1,
                transactional = IS_TRANSACTIONAL,
                prefix = slotMessageV2.prefix!!,
                content = slotMessageV2.content!!,
                lsn = slotMessageV2.lsn,
            )

            else -> buildOtherChange(slotMessageV2)
        }

        return listOf(change)
    }

    private fun buildOtherChange(message: SlotMessageV2): Change {
        return when (message.action.uppercase()) {
            "I" -> InsertChange(
                kindInput = INSERT_TYPE_V1,
                schema = message.schema,
                table = message.table,
                lsn = message.lsn,
                columns = message.columns,
            )
            "U" -> UpdateChange(
                kindInput = UPDATE_TYPE_V1,
                schema = message.schema,
                table = message.table,
                lsn = message.lsn,
                columns = message.columns,
                identity = message.identity,
            )
            "D" -> DeleteChange(
                kindInput = DELETE_TYPE_V1,
                schema = message.schema,
                table = message.table,
                lsn = message.lsn,
                identity = message.identity,
            )
            else -> Change(OTHER_TYPE_V1)
        }
    }

    companion object {
        const val INSERT_TYPE_V1 = "insert"
        const val UPDATE_TYPE_V1 = "update"
        const val DELETE_TYPE_V1 = "delete"
        const val MESSAGE_TYPE_V1 = "message"
        const val OTHER_TYPE_V1 = "other"
        const val MESSAGE_TYPE_V2 = "M"
        const val IS_TRANSACTIONAL = true
    }
}
