package shop.inventa.pg2sns4k.aws.sns

import io.awspring.cloud.messaging.core.NotificationMessagingTemplate
import org.springframework.stereotype.Component
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage

@Component
class SNSTransactionalProducer(
    private val notificationMessagingTemplate: NotificationMessagingTemplate
) : SNSProducer {

    override fun <T : Any> send(topicName: String, message: SNSMessage<T>) {
        notificationMessagingTemplate.convertAndSend(topicName, message.body, message.headers)
    }
}
