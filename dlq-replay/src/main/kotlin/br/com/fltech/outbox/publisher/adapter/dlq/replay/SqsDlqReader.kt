package br.com.fltech.outbox.publisher.adapter.dlq.replay

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.QueueAttributeName
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import software.amazon.awssdk.services.sqs.model.SqsException

/**
 * SQS-backed [DlqReader]. Peek uses a short `VisibilityTimeout` so
 * the operator can list messages without locking them out for long
 * — the trade-off is documented in the constructor parameter.
 *
 * The reader is read-only over the AWS SDK client; it never sends
 * messages back to the queue. Replay re-publishes through the
 * `EventSinkRegistry` and then calls [delete] once the publish has
 * succeeded.
 */
class SqsDlqReader(
    private val sqs: SqsClient,
    private val queueName: String,
    /**
     * How long a peeked message stays invisible to other consumers
     * before re-appearing on the queue. Short by design so a crashed
     * operator process does not lock messages out. Default 5s.
     */
    private val peekVisibilityTimeoutSeconds: Int = DEFAULT_PEEK_VISIBILITY_SECONDS,
    private val objectMapper: ObjectMapper = jacksonObjectMapper(),
) : DlqReader {
    private val queueUrl: String by lazy {
        sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl()
    }

    override fun peek(max: Int): List<DlqReader.Message> {
        val bounded = max.coerceIn(1, MAX_RECEIVE_BATCH)
        val response =
            sqs.receiveMessage(
                ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(bounded)
                    .visibilityTimeout(peekVisibilityTimeoutSeconds)
                    .waitTimeSeconds(0)
                    .build(),
            )
        return response.messages().mapNotNull { sqsMessage ->
            try {
                val envelope = objectMapper.readValue(sqsMessage.body(), DlqEnvelope::class.java)
                DlqReader.Message(DlqReader.Handle(sqsMessage.receiptHandle()), envelope)
            } catch (e: Exception) {
                logger.warn(
                    "SqsDlqReader: dropped malformed DLQ message (queue={}, messageId={}, cause={}); leaving on queue.",
                    queueName,
                    sqsMessage.messageId(),
                    e.javaClass.simpleName,
                    e,
                )
                null
            }
        }
    }

    override fun delete(handle: DlqReader.Handle) {
        try {
            sqs.deleteMessage(
                DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(handle.value)
                    .build(),
            )
        } catch (e: SqsException) {
            logger.warn(
                "SqsDlqReader: deleteMessage failed for queue={} (cause={}); " +
                    "message may resurface after visibility timeout.",
                queueName,
                e.javaClass.simpleName,
                e,
            )
            throw e
        }
    }

    override fun stats(): DlqReader.Stats {
        val response =
            sqs.getQueueAttributes(
                GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                    )
                    .build(),
            )
        val attrs = response.attributesAsStrings()
        return DlqReader.Stats(
            approximateMessageCount =
                attrs[QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES.toString()]?.toLongOrNull() ?: 0L,
            approximateMessageNotVisibleCount =
                attrs[QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE.toString()]?.toLongOrNull() ?: 0L,
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SqsDlqReader::class.java)
        private const val DEFAULT_PEEK_VISIBILITY_SECONDS = 5
        private const val MAX_RECEIVE_BATCH = 10
    }
}
