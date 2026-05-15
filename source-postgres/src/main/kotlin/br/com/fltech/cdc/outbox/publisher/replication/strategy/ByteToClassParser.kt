package br.com.fltech.cdc.outbox.publisher.replication.strategy

import br.com.fltech.cdc.outbox.publisher.replication.model.Change
import java.nio.ByteBuffer

interface ByteToClassParser {

    fun parse(byteBufferMessage: ByteBuffer): List<Change>
}
