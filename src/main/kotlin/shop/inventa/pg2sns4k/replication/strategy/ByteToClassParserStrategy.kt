package shop.inventa.pg2sns4k.replication.strategy

import org.springframework.stereotype.Component
import shop.inventa.pg2sns4k.replication.config.ReplicationConfiguration
import shop.inventa.pg2sns4k.replication.enums.FormatVersionEnum

@Component
class ByteToClassParserStrategy(
    private val jsonToClassParserImplV1: ByteToClassParserImplV1,
    private val jsonToClassParserImplV2: ByteToClassParserImplV2
) {

    fun selectParser(replicationConfiguration: ReplicationConfiguration): ByteToClassParser {
        return when (replicationConfiguration.formatVersion) {
            FormatVersionEnum.V1 -> jsonToClassParserImplV1
            FormatVersionEnum.V2 -> jsonToClassParserImplV2
            else -> throw IllegalArgumentException("Unknown format version: ${replicationConfiguration.formatVersion}")
        }
    }
}
