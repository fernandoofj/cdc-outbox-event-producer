package shop.inventa.pg2sns4k.aws.sns

import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage

interface SNSProducer {

    fun <T : Any> send(topicName: String, message: shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage<T>)
}
