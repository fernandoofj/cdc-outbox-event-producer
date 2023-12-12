package shop.inventa.pg2sns4k.workflow

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage
import shop.inventa.pg2sns4k.replication.connector.PostgresConnector

@Component
class SlotReaderCallback(
    private val slotReaderSNSProducer: SlotReaderSNSProducer,
    private val postgresConnector: PostgresConnector
) {

    fun onFailure(topicName: String, t: Throwable) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.error("Failed to send record $lsn to SNS #$topicName", t)
    }

    fun onSuccess(topicName: String, message: SNSMessage<Any>) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.info(
            "Successfully sent record $lsn " +
                "containing event #${message.body.eventUUID} " +
                "of type #${message.body.eventType} " +
                "and domainId #${message.body.domainId} " +
                "to SNS #$topicName"
        )

        postgresConnector.setStreamLsn(lsn)
        slotReaderSNSProducer.resetIdleCounter()
    }

    fun discardMessage(type: String) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.info("Discarding record $lsn type #$type")

        postgresConnector.setStreamLsn(lsn)
        slotReaderSNSProducer.resetIdleCounter()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SlotReaderCallback::class.java)
    }
}
