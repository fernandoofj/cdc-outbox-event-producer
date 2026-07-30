package br.com.fltech.outbox.publisher.core.port

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing

/**
 * Routing-scheme → [EventSink] resolver.
 *
 * The orchestrator hands every event to the registry; the registry
 * looks up the right sink for the event's `routing.scheme` and
 * delegates. This is what replaces the hard-coded
 * `when (DestinationType.SNS|SQS)` switch in the original implementation
 * — adding a new broker becomes "register an EventSink under a new
 * scheme" instead of editing an enum + switch in the producer.
 */
interface EventSinkRegistry {
    /**
     * Return the sink registered for [scheme], or `null` if no sink
     * handles that scheme. Implementations should be cheap; callers may
     * invoke this on every event.
     */
    fun resolve(scheme: String): EventSink?

    /** Convenience: resolve + publish + raise if no sink handles the scheme. */
    fun publish(routing: Routing, event: OutboxEvent) {
        val sink = resolve(routing.scheme)
            ?: throw NoSinkForSchemeException(routing.scheme, knownSchemes())
        sink.publish(routing, event)
    }

    /** All schemes the registry currently knows about (for diagnostics). */
    fun knownSchemes(): Set<String>
}

/**
 * Thrown by [EventSinkRegistry.publish] when no sink is registered for
 * the event's [scheme]. The orchestrator's error handling treats this
 * as a non-transient publish failure — retries won't help.
 */
class NoSinkForSchemeException(
    val scheme: String,
    val knownSchemes: Set<String>,
) : RuntimeException(
    "No EventSink registered for scheme '$scheme'. " +
        "Known schemes: ${knownSchemes.sorted()}",
)
