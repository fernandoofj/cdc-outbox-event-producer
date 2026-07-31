package br.com.fltech.outbox.publisher.workflow

import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.outbox.publisher.replication.connector.PostgresConnector
import org.postgresql.replication.LogSequenceNumber
import org.slf4j.LoggerFactory

/**
 * Outcome-handling for one WAL message, owning the LSN-advancement decision.
 *
 * The caller is required to pass the LSN of the message being acted on as the
 * first parameter of every method. This is intentional: in the original
 * implementation the callback read `PostgresConnector.lastReceivedLsn()` at
 * success/failure time, but that value drifts ahead of the in-flight message
 * as the replication stream buffers more data. A failure followed by a
 * success therefore silently advanced the slot past the failed message,
 * dropping it. The fix is to capture the LSN at read time in the caller
 * and thread it through.
 *
 * Invariants enforced here:
 *  - On success or explicit discard, the slot LSN is advanced to the captured
 *    LSN of THAT message — never `lastReceivedLsn()`.
 *  - On failure, the slot is NOT advanced. Postgres will redeliver the same
 *    message on the next read, preserving at-least-once delivery.
 */
class SlotReaderCallback(
    private val slotReaderMessageProducer: SlotReaderMessageProducer,
    private val postgresConnector: PostgresConnector,
    private val metrics: CdcOutboxMetrics = CdcOutboxMetrics.noop(),
) {
    fun onFailure(
        lsn: LogSequenceNumber,
        prefix: String,
        sink: String,
        t: Throwable,
    ) {
        logger.error(
            "Failed to send record {} to #{} via {} — slot will not advance",
            lsn,
            prefix,
            sink,
            t,
        )
        metrics.recordFailure(sink = sink, topic = prefix, cause = t.javaClass.simpleName)
        // Intentional: do NOT call setStreamLsn. The slot stays put so
        // Postgres redelivers this exact message on the next read.
    }

    fun onSNSSuccess(
        lsn: LogSequenceNumber,
        destinationName: String,
        message: SNSMessage<Any>,
    ) {
        logger.info(
            "Successfully sent record {} containing event #{} of type #{} and domainId #{} to SNS topic #{}",
            lsn,
            message.body.eventUUID,
            message.body.eventType,
            message.body.domainId,
            destinationName,
        )
        metrics.recordPublished(sink = SINK_SNS, topic = destinationName)
        finishWithSuccess(lsn)
    }

    fun onSQSSuccess(
        lsn: LogSequenceNumber,
        destinationName: String,
    ) {
        logger.info("Successfully sent record {} to SQS queue #{}", lsn, destinationName)
        metrics.recordPublished(sink = SINK_SQS, topic = destinationName)
        finishWithSuccess(lsn)
    }

    fun discardMessage(
        lsn: LogSequenceNumber,
        type: String,
    ) {
        logger.info("Discarding record {} type #{}", lsn, type)
        metrics.recordDiscarded(reason = type)
        finishWithSuccess(lsn)
    }

    private fun finishWithSuccess(lsn: LogSequenceNumber) {
        postgresConnector.setStreamLsn(lsn)
        slotReaderMessageProducer.resetIdleCounter()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SlotReaderCallback::class.java)
        const val SINK_SNS = "sns"
        const val SINK_SQS = "sqs"
    }
}
