package br.com.fltech.cdc.outbox.publisher.adapter.sink.sqs

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.Routing
import br.com.fltech.cdc.outbox.publisher.core.port.EventSink
import io.awspring.cloud.sqs.operations.SqsTemplate

/**
 * EventSink backed by Spring Cloud AWS 3.x's [SqsTemplate]. Registered
 * under the `sqs` routing scheme by the auto-configuration.
 *
 * The event payload flows as the SQS message body. Event headers +
 * routing attributes are sent as SQS message attributes via the
 * `headers(map)` configurer; routing attributes override event headers
 * on key collisions.
 */
class SqsEventSink(
    private val sqsTemplate: SqsTemplate,
) : EventSink {

    override fun publish(routing: Routing, event: OutboxEvent) {
        val attributes: Map<String, Any> = event.headers + routing.attributes
        sqsTemplate.send { it.queue(routing.target).payload(event.payloadAsString).headers(attributes) }
    }
}
