package br.com.fltech.outbox.publisher.adapter.sink.kafka

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSink
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.KafkaTemplate

/**
 * EventSink backed by Spring Kafka's [KafkaTemplate]. Registered under
 * the `kafka` routing scheme by the auto-configuration.
 *
 * `routing.target` is the topic name. The event's `id` is used as the
 * record key so events with the same domain id land on the same
 * partition (preserves per-entity ordering at the consumer side).
 * Event headers + routing attributes are written as Kafka record
 * headers; routing attributes win on key collisions.
 *
 * The publish is synchronous: `KafkaTemplate.send(...).get()` blocks
 * until the broker acknowledges. That matches the at-least-once
 * contract — the orchestrator's `ack` only fires after this method
 * returns.
 */
class KafkaEventSink(
    private val kafkaTemplate: KafkaTemplate<String, ByteArray>,
) : EventSink {
    override fun publish(
        routing: Routing,
        event: OutboxEvent,
    ) {
        val record = ProducerRecord<String, ByteArray>(routing.target, event.id, event.payload)
        val merged = event.headers + routing.attributes
        merged.forEach { (k, v) ->
            record.headers().add(RecordHeader(k, v.toByteArray(Charsets.UTF_8)))
        }
        // Block until the broker acks. Spring Kafka's CompletableFuture
        // is non-blocking by default; we want backpressure here so the
        // orchestrator's retry / DLQ state machine sees a real failure.
        kafkaTemplate.send(record).get()
    }
}
