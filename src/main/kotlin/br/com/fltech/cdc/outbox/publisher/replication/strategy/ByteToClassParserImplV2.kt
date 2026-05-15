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

@Component
class ByteToClassParserImplV2(
    private val defaultMapper: ObjectMapper
) : ByteToClassParser {
    override fun parse(byteBufferMessage: ByteBuffer): List<Change> {
        val byteArray = ByteArray(byteBufferMessage.remaining())
        byteBufferMessage.get(byteArray)
        val jsonString = String(byteArray, Charsets.UTF_8)
        val slotMessageV2 = defaultMapper.readValue(jsonString, SlotMessageV2::class.java)

        val messageChange = slotMessageV2.takeIf { it.action == MESSAGE_TYPE_V2 }
            ?.let {
                MessageChange(
                    kindInput = MESSAGE_TYPE_V1,
                    transactional = IS_TRANSACTIONAL,
                    prefix = slotMessageV2.prefix!!,
                    content = slotMessageV2.content!!
                )
            } ?: buildOtherChange(slotMessageV2.action)

        return listOf(messageChange)
    }

    private fun buildOtherChange(action: String): Change {
        return when (action.uppercase()) {
            "I" -> InsertChange(kindInput = INSERT_TYPE_V1)
            "U" -> UpdateChange(kindInput = UPDATE_TYPE_V1)
            "D" -> DeleteChange(kindInput = DELETE_TYPE_V1)
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
