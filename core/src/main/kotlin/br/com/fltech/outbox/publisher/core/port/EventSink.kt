package br.com.fltech.outbox.publisher.core.port

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing

/**
 * Driven (output) port for the orchestrator: publishes an [OutboxEvent]
 * to a concrete broker.
 *
 * Implementations:
 *  - `adapter.sink.sns.SnsEventSink`        — SCA 3 `SnsTemplate`.
 *  - `adapter.sink.sqs.SqsEventSink`        — SCA 3 `SqsTemplate`.
 *  - `adapter.sink.kafka.KafkaEventSink`    — Spring Kafka `KafkaTemplate`.
 *  - `adapter.sink.rabbitmq.RabbitMqEventSink` — Spring AMQP `RabbitTemplate`.
 *  - `adapter.sink.composite.CompositeEventSink` — fan-out to several
 *    sinks at once (dual-write during a migration).
 *  - `adapter.sink.router.SchemeRouterEventSink` — picks the underlying
 *    sink based on the event's `routing.scheme`.
 *
 * Each sink is bound to ONE [Routing.scheme] (`sns`, `sqs`, `kafka`, …)
 * via the [EventSinkRegistry]. Implementations MUST raise on permanent
 * failure so the orchestrator can drive its retry / dead-letter state
 * machine; transient retries belong INSIDE the orchestrator, not inside
 * the sink (so the policy is uniform across brokers).
 */
fun interface EventSink {
    /**
     * Publish [event] to [routing.target]. Throws on failure; the
     * orchestrator decides retry / dead-letter.
     */
    fun publish(
        routing: Routing,
        event: OutboxEvent,
    )
}
