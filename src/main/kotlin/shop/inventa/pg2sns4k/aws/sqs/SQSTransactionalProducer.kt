package shop.inventa.pg2sns4k.aws.sqs

import io.awspring.cloud.messaging.core.QueueMessagingTemplate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class SQSTransactionalProducer(
    private val queueMessagingTemplate: QueueMessagingTemplate
):SQSProducer {
    override fun <T : Any> send(queueName: String, message: T) {
        queueMessagingTemplate.convertAndSend(queueName, message)
    }
}
