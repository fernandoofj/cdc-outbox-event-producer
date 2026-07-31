package br.com.fltech.outbox.publisher.replication.strategy

import br.com.fltech.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.outbox.publisher.replication.enums.FormatVersionEnum
import org.springframework.stereotype.Component

@Component
class ByteToClassParserStrategy(
    private val jsonToClassParserImplV1: ByteToClassParserImplV1,
    private val jsonToClassParserImplV2: ByteToClassParserImplV2,
) {
    fun selectParser(replicationConfiguration: ReplicationConfiguration): ByteToClassParser {
        return when (replicationConfiguration.formatVersion) {
            FormatVersionEnum.V1 -> jsonToClassParserImplV1
            FormatVersionEnum.V2 -> jsonToClassParserImplV2
        }
    }
}
