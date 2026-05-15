package br.com.fltech.cdc.outbox.publisher.aws.sns

import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import io.awspring.cloud.sns.core.SnsTemplate

/**
 * SNS producer adapter backed by Spring Cloud AWS 3.x's [SnsTemplate].
 *
 * SCA 3 dropped the SCA-2 `NotificationMessagingTemplate` together with
 * AWS SDK v1; [SnsTemplate] is its replacement and runs on AWS SDK v2.
 * The public surface of this class is unchanged — the swap is internal.
 * Spring annotations live in the auto-configuration; this class itself
 * is a plain Kotlin POJO so the library jar remains usable without
 * Spring on the classpath.
 */
class SNSTransactionalProducer(
    private val snsTemplate: SnsTemplate,
) : SNSProducer {

    override fun <T : Any> send(topicName: String, message: SNSMessage<T>) {
        snsTemplate.convertAndSend(topicName, message.body, message.headers)
    }
}
