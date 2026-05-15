package br.com.fltech.cdc.outbox.publisher.adapter.sink.composite

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.Routing
import br.com.fltech.cdc.outbox.publisher.core.port.EventSink

/**
 * Fan-out [EventSink] that delegates every publish to every wrapped
 * sink in order.
 *
 * Useful during a broker migration: bind the same scheme to a
 * `CompositeEventSink(currentBroker, newBroker)` so events land on
 * both backends until the cutover is complete. After the orchestrator's
 * publish succeeds, the source `ack` advances exactly once — there is
 * no per-sink ack here.
 *
 * Failure semantics:
 *  - `failFast = true`  (default): the first sink that throws aborts
 *    the publish. The orchestrator's retry will re-run ALL sinks,
 *    including the ones that already succeeded — at-least-once,
 *    consumers must dedup.
 *  - `failFast = false`: every sink is invoked even if earlier ones
 *    threw, and the first captured exception is re-thrown at the end.
 *    Useful when sinks are independent and you'd rather deliver to as
 *    many as possible per attempt.
 */
class CompositeEventSink(
    private val delegates: List<EventSink>,
    private val failFast: Boolean = true,
) : EventSink {

    init {
        require(delegates.isNotEmpty()) { "CompositeEventSink requires at least one delegate" }
    }

    // TooGenericExceptionCaught: in fan-out mode we must aggregate
    // failures across all delegates rather than short-circuit; catching
    // Throwable is intentional so an Error from one sink (broker SDK
    // bug, OOM mid-publish) still lets the rest try and aggregates
    // into a suppressed-exception chain.
    @Suppress("TooGenericExceptionCaught")
    override fun publish(routing: Routing, event: OutboxEvent) {
        if (failFast) {
            delegates.forEach { it.publish(routing, event) }
            return
        }
        var firstFailure: Throwable? = null
        delegates.forEach { sink ->
            try {
                sink.publish(routing, event)
            } catch (t: Throwable) {
                if (firstFailure == null) firstFailure = t else firstFailure!!.addSuppressed(t)
            }
        }
        firstFailure?.let { throw it }
    }
}
