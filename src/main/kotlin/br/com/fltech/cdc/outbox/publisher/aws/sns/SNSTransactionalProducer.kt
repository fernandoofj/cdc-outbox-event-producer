package br.com.fltech.cdc.outbox.publisher.aws.sns

import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import io.awspring.cloud.messaging.core.NotificationMessagingTemplate
import org.springframework.stereotype.Component

@Component
class SNSTransactionalProducer(
    private val notificationMessagingTemplate: NotificationMessagingTemplate
) : SNSProducer {

    override fun <T : Any> send(topicName: String, message: SNSMessage<T>) {
        notificationMessagingTemplate.convertAndSend(topicName, message.body, message.headers)
    }
}
