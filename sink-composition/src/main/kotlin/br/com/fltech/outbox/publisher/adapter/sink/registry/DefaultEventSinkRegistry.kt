package br.com.fltech.outbox.publisher.adapter.sink.registry

import br.com.fltech.outbox.publisher.core.port.EventSink
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry

/**
 * Immutable [EventSinkRegistry] backed by a `scheme → EventSink` map.
 *
 * Construct once during application start-up from whichever sinks are
 * present on the classpath. The Spring auto-configuration uses this
 * class; non-Spring callers can build it by hand.
 */
class DefaultEventSinkRegistry(
    sinks: Map<String, EventSink>,
) : EventSinkRegistry {

    private val sinks: Map<String, EventSink> = sinks.mapKeys { (k, _) -> k.lowercase() }

    init {
        require(this.sinks.keys.all { it.isNotBlank() }) { "sink scheme cannot be blank" }
    }

    override fun resolve(scheme: String): EventSink? = sinks[scheme.lowercase()]

    override fun knownSchemes(): Set<String> = sinks.keys
}
