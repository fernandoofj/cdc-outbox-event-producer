package shop.inventa.pg2sns4k.workflow

import org.slf4j.LoggerFactory
import shop.inventa.pg2sns4k.replication.connector.PostgresConnector
import java.util.UUID

class SlotReaderCallback constructor(
    private val slotReaderSNSProducer: SlotReaderSNSProducer,
    private val postgresConnector: PostgresConnector,
) {

    fun onFailure(topicName: String, t: Throwable) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.error("Failed to send record {} to SNS {}", lsn, topicName, t)
    }

    fun onSuccess(topicName: String, eventType: String, eventUUID: UUID) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.info("Successfully sent event {} of type {} to SNS {}", eventUUID, eventType, topicName)

        postgresConnector.setStreamLsn(lsn)
        slotReaderSNSProducer.resetIdleCounter()
    }

    fun discardMessage(type: String) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.info("Discarding record {} type {}", lsn, type)

        postgresConnector.setStreamLsn(lsn)
        slotReaderSNSProducer.resetIdleCounter()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SlotReaderCallback::class.java)
    }
}
