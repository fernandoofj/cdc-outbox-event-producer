package shop.inventa.pg2sns4k.replication.strategy

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import shop.inventa.pg2sns4k.replication.model.Change
import shop.inventa.pg2sns4k.replication.model.SlotMessageV1
import java.nio.ByteBuffer

@Component
class ByteToClassParserImplV1(
    private val defaultMapper: ObjectMapper
) : ByteToClassParser {

    override fun parse(byteBufferMessage: ByteBuffer): List<Change> {
        val byteArray = ByteArray(byteBufferMessage.remaining())
        byteBufferMessage.get(byteArray)
        val jsonString = String(byteArray, Charsets.UTF_8)
        val slotMessageV1 = defaultMapper.readValue(jsonString, SlotMessageV1::class.java)
        return slotMessageV1.changes
    }
}
