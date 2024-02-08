package shop.inventa.pg2sns4k.aws.sqs

import io.awspring.cloud.messaging.core.QueueMessagingTemplate
import org.springframework.beans.factory.annotation.Autowired
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage

interface SQSProducer {
    fun <T : Any> send(queueName: String, message: T)
}
