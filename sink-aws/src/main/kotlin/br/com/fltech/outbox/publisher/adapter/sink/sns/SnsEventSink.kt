package br.com.fltech.outbox.publisher.adapter.sink.sns

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSink
import io.awspring.cloud.sns.core.SnsTemplate

/**
 * EventSink backed by Spring Cloud AWS 3.x's [SnsTemplate]. Registered
 * under the `sns` routing scheme by the auto-configuration.
 *
 * The event payload flows as the SNS message body (as a UTF-8 string —
 * SNS messages are textual). Event headers + routing attributes are
 * merged into the SNS message-attributes map; the routing attributes
 * win on key collisions (caller is more authoritative than upstream).
 */
class SnsEventSink(
    private val snsTemplate: SnsTemplate,
) : EventSink {

    override fun publish(routing: Routing, event: OutboxEvent) {
        val attributes: Map<String, Any> = event.headers + routing.attributes
        snsTemplate.convertAndSend(routing.target, event.payloadAsString, attributes)
    }
}
