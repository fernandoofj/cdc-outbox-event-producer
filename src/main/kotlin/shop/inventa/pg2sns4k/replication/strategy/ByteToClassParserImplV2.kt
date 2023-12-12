package shop.inventa.pg2sns4k.replication.strategy

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import shop.inventa.pg2sns4k.replication.model.Change
import shop.inventa.pg2sns4k.replication.model.DeleteChange
import shop.inventa.pg2sns4k.replication.model.InsertChange
import shop.inventa.pg2sns4k.replication.model.MessageChange
import shop.inventa.pg2sns4k.replication.model.SlotMessageV1
import shop.inventa.pg2sns4k.replication.model.SlotMessageV2
import shop.inventa.pg2sns4k.replication.model.UpdateChange
import java.nio.ByteBuffer

@Component
class ByteToClassParserImplV2(
    private val defaultMapper: ObjectMapper
) : ByteToClassParser {
    override fun parse(byteBufferMessage: ByteBuffer): SlotMessageV1 {
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

        return SlotMessageV1(
            slotMessageV2.xid,
            listOf(messageChange)
        )
    }

    private fun buildOtherChange(action: String): Change {
        return when (action.uppercase()) {
            "I" -> InsertChange(kindInput = "insert")
            "U" -> UpdateChange(kindInput = "insert")
            "D" -> DeleteChange(kindInput = "insert")
            else -> Change(OTHER_TYPE_V1)
        }
    }

    companion object {
        const val MESSAGE_TYPE_V2 = "M"
        const val MESSAGE_TYPE_V1 = "message"
        const val OTHER_TYPE_V1 = "other"
        const val IS_TRANSACTIONAL = true
    }
}
