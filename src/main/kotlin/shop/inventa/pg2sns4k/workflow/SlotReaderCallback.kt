package shop.inventa.pg2sns4k.workflow

import org.postgresql.replication.LogSequenceNumber
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage
import shop.inventa.pg2sns4k.replication.connector.PostgresConnector

@Component
class SlotReaderCallback(
    private val slotReaderMessageProducer: SlotReaderMessageProducer,
    private val postgresConnector: PostgresConnector
) {

    fun onFailure(prefix: String, t: Throwable) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.error("Failed to send record $lsn to #$prefix", t)
    }

    fun onSNSSuccess(destinationName: String, message: SNSMessage<Any>) {
        val lsn = postgresConnector.lastReceivedLsn()

        logger.info(
            "Successfully sent record $lsn " +
                "containing event #${message.body.eventUUID} " +
                "of type #${message.body.eventType} " +
                "and domainId #${message.body.domainId} " +
                "to SNS topic #$destinationName"
        )

        finishWithSuccess(lsn)
    }

    fun onSQSSuccess(destinationName: String) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.info(
            "Successfully sent record $lsn to SQS queue #$destinationName"
        )

        finishWithSuccess(lsn)
    }

    private fun finishWithSuccess(lsn: LogSequenceNumber) {
        postgresConnector.setStreamLsn(lsn)
        slotReaderMessageProducer.resetIdleCounter()
    }

    fun discardMessage(type: String) {
        val lsn = postgresConnector.lastReceivedLsn()
        logger.info("Discarding record $lsn type #$type")

        postgresConnector.setStreamLsn(lsn)
        slotReaderMessageProducer.resetIdleCounter()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SlotReaderCallback::class.java)
    }
}
