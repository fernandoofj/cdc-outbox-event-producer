package br.com.fltech.outbox.publisher.adapter.sink.router

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSink
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry

/**
 * [EventSink] adapter that re-routes through an [EventSinkRegistry].
 *
 * Useful when you want to register ONE sink under multiple schemes
 * but still have per-event routing decided by the event's
 * `routing.scheme`. The orchestrator uses the registry directly in the
 * common case; this wrapper is for the unusual case where a composite
 * pipeline (e.g. `CompositeEventSink`) needs to route some events one
 * way and others another way.
 */
class SchemeRouterEventSink(
    private val registry: EventSinkRegistry,
) : EventSink {
    override fun publish(routing: Routing, event: OutboxEvent) {
        registry.publish(routing, event)
    }
}
