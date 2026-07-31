package br.com.fltech.outbox.publisher.replication.strategy

import br.com.fltech.outbox.publisher.replication.model.Change
import java.nio.ByteBuffer

interface ByteToClassParser {
    fun parse(byteBufferMessage: ByteBuffer): List<Change>
}
