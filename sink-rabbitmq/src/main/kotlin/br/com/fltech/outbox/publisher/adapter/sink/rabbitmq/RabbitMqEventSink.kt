package br.com.fltech.outbox.publisher.adapter.sink.rabbitmq

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSink
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageBuilder
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate

/**
 * EventSink backed by Spring AMQP's [RabbitTemplate]. Registered under
 * the `amqp` routing scheme by the auto-configuration.
 *
 * `routing.target` is parsed as `exchange/routingKey` — a single slash
 * separates the two. An empty exchange (`/queue`) publishes through
 * the default exchange, which routes by queue name. Event headers +
 * routing attributes become `MessageProperties` headers; routing
 * attributes win on key collisions. `messageId` is the event id, so
 * downstream consumers can deduplicate.
 *
 * Publish is synchronous (RabbitTemplate.send blocks). The
 * orchestrator's retry / DLQ state machine catches the exception when
 * the broker rejects.
 */
class RabbitMqEventSink(
    private val rabbitTemplate: RabbitTemplate,
) : EventSink {

    override fun publish(routing: Routing, event: OutboxEvent) {
        val (exchange, routingKey) = parseTarget(routing.target)
        val merged = event.headers + routing.attributes
        val properties = MessageProperties().apply {
            messageId = event.id
            timestamp = java.util.Date.from(event.occurredAt)
            merged.forEach { (k, v) -> setHeader(k, v) }
        }
        val message: Message = MessageBuilder.withBody(event.payload).andProperties(properties).build()
        rabbitTemplate.send(exchange, routingKey, message)
    }

    private fun parseTarget(target: String): Pair<String, String> {
        val idx = target.indexOf('/')
        return if (idx < 0) {
            // Bare target → publish through the default exchange with target as the routing key.
            "" to target
        } else {
            target.substring(0, idx) to target.substring(idx + 1)
        }
    }
}
