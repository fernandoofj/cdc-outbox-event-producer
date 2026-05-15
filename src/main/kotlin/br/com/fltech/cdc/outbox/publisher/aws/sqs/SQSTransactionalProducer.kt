package br.com.fltech.cdc.outbox.publisher.aws.sqs

import io.awspring.cloud.messaging.core.QueueMessagingTemplate
import org.springframework.stereotype.Component

@Component
class SQSTransactionalProducer(
    private val queueMessagingTemplate: QueueMessagingTemplate
) : SQSProducer {
    override fun <T : Any> send(queueName: String, message: T) {
        queueMessagingTemplate.convertAndSend(queueName, message)
    }
}
