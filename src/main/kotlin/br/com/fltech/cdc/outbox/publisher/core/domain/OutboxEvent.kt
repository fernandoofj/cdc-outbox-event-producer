package br.com.fltech.cdc.outbox.publisher.core.domain

import java.time.Instant

/**
 * The single value type that crosses the hexagon boundary.
 *
 * `OutboxEvent` is what a [br.com.fltech.cdc.outbox.publisher.core.port.CdcSource]
 * adapter produces and what a
 * [br.com.fltech.cdc.outbox.publisher.core.port.EventSink] adapter consumes.
 * Source adapters translate database-native shapes (wal2json messages,
 * binlog row events, outbox-table rows) into this type; sink adapters
 * translate the same type into broker-native shapes (SNS notifications,
 * Kafka records, AMQP messages).
 *
 * Invariants:
 *  - `id` is unique within the source scope (LSN, GTID, or row PK) and is
 *    what the processor reports to `ack` so the source can advance its
 *    checkpoint. It MUST be deterministic on redelivery so dedup downstream
 *    is possible.
 *  - `payload` is opaque to the processor — adapters decide their own
 *    serialisation. The contract is "bytes go in, bytes come out".
 *  - `headers` are propagated to brokers that support them (SNS message
 *    attributes, Kafka headers, AMQP headers). Sinks that don't support
 *    headers are allowed to drop them, but MUST document it.
 */
data class OutboxEvent(
    /** Unique within the source scope. The processor passes this back to `ack`. */
    val id: String,
    /** Where this event should be delivered. */
    val routing: Routing,
    /** Raw payload. Adapters own encoding. */
    val payload: ByteArray,
    /** When the event was produced upstream (commit timestamp, not read time). */
    val occurredAt: Instant,
    /** Source-specific checkpoint marker (LSN, GTID, etc.). Opaque to sinks. */
    val sourceCheckpoint: String,
    /** Optional message-level headers / attributes. */
    val headers: Map<String, String> = emptyMap(),
) {
    /** Lazy UTF-8 view of the payload for adapters that prefer Strings. */
    val payloadAsString: String by lazy { String(payload, Charsets.UTF_8) }

    /**
     * `equals`/`hashCode` are overridden so the test machinery and any
     * collection that deduplicates events behaves as the value-type
     * contract demands — the auto-generated data-class equals would compare
     * the `payload` array by reference.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as OutboxEvent
        return id == other.id &&
            routing == other.routing &&
            payload.contentEquals(other.payload) &&
            occurredAt == other.occurredAt &&
            sourceCheckpoint == other.sourceCheckpoint &&
            headers == other.headers
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + routing.hashCode()
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + occurredAt.hashCode()
        result = 31 * result + sourceCheckpoint.hashCode()
        result = 31 * result + headers.hashCode()
        return result
    }
}
