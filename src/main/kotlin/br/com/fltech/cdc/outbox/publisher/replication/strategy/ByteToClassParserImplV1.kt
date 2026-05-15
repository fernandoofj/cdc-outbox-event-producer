package br.com.fltech.cdc.outbox.publisher.replication.strategy

import br.com.fltech.cdc.outbox.publisher.replication.model.Change
import br.com.fltech.cdc.outbox.publisher.replication.model.SlotMessageV1
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.ByteBuffer
import org.springframework.stereotype.Component

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
