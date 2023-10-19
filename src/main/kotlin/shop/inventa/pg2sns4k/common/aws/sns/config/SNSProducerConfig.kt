package shop.inventa.pg2sns4k.common.aws.sns.config

import com.amazonaws.services.sns.AmazonSNS
import io.awspring.cloud.messaging.core.NotificationMessagingTemplate
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.converter.MappingJackson2MessageConverter

@Configuration
class SNSProducerConfig(
    private val amazonSNS: AmazonSNS,
    private val jackson2MessageConverter: MappingJackson2MessageConverter
) {
    @Bean
    fun notificationMessagingTemplate() = NotificationMessagingTemplate(amazonSNS).apply {
        messageConverter = jackson2MessageConverter
    }
}
