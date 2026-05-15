package shop.inventa.pg2sns4k.replication.strategy

import shop.inventa.pg2sns4k.replication.model.Change
import java.nio.ByteBuffer

interface ByteToClassParser {

    fun parse(byteBufferMessage: ByteBuffer): List<Change>
}
