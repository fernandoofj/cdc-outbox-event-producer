package br.com.fltech.cdc.outbox.publisher.observability

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.time.Duration

/**
 * Thin Micrometer facade for the CDC-outbox pipeline.
 *
 * When [registry] is `null` all calls are no-ops; this is the path used by
 * library consumers that do not wire Micrometer. When a registry is present,
 * counters and timers are registered lazily on first use and cached by name
 * + tag set by Micrometer itself.
 *
 * Metric names follow Prometheus/OpenMetrics conventions
 * (`cdc.outbox.<subject>.<action>`) — Micrometer adds the `_total` suffix to
 * counters and the `_seconds` family to timers on export.
 */
class CdcOutboxMetrics(private val registry: MeterRegistry?) {

    fun recordMessageRead(slot: String) {
        registry?.let {
            Counter.builder(MESSAGES_READ)
                .description("WAL messages read from the replication slot")
                .tags(Tags.of(TAG_SLOT, slot))
                .register(it)
                .increment()
        }
    }

    fun recordPublished(sink: String, topic: String) {
        registry?.let {
            Counter.builder(MESSAGES_PUBLISHED)
                .description("Events successfully published to a sink")
                .tags(Tags.of(TAG_SINK, sink, TAG_TOPIC, topic))
                .register(it)
                .increment()
        }
    }

    fun recordFailure(sink: String, topic: String, cause: String) {
        registry?.let {
            Counter.builder(MESSAGES_FAILED)
                .description("Publish attempts that raised an exception")
                .tags(Tags.of(TAG_SINK, sink, TAG_TOPIC, topic, TAG_CAUSE, cause))
                .register(it)
                .increment()
        }
    }

    fun recordDiscarded(reason: String) {
        registry?.let {
            Counter.builder(MESSAGES_DISCARDED)
                .description("WAL records intentionally discarded (non-message kinds, empty payloads, etc.)")
                .tags(Tags.of(TAG_REASON, reason))
                .register(it)
                .increment()
        }
    }

    fun recordPublishDuration(sink: String, duration: Duration) {
        registry?.let {
            Timer.builder(PUBLISH_DURATION)
                .description("End-to-end publish latency from WAL message to sink ack")
                .tags(Tags.of(TAG_SINK, sink))
                .register(it)
                .record(duration)
        }
    }

    fun recordReconnect(reason: String) {
        registry?.let {
            Counter.builder(RECONNECTS)
                .description("Replication-stream reconnect attempts grouped by cause")
                .tags(Tags.of(TAG_REASON, reason))
                .register(it)
                .increment()
        }
    }

    fun recordRetry(sink: String, topic: String, attempt: Int) {
        registry?.let {
            Counter.builder(PUBLISH_RETRIES)
                .description("Publish-retry attempts (does not include the initial try)")
                .tags(Tags.of(TAG_SINK, sink, TAG_TOPIC, topic, TAG_ATTEMPT, attempt.toString()))
                .register(it)
                .increment()
        }
    }

    fun recordDeadLettered(sink: String, topic: String, cause: String) {
        registry?.let {
            Counter.builder(DEAD_LETTERED)
                .description("Messages forwarded to the dead-letter sink after exhausting retries")
                .tags(Tags.of(TAG_SINK, sink, TAG_TOPIC, topic, TAG_CAUSE, cause))
                .register(it)
                .increment()
        }
    }

    fun recordDeadLetterFailure(cause: String) {
        registry?.let {
            Counter.builder(DEAD_LETTER_FAILURES)
                .description("Dead-letter publishes that themselves raised an exception — operator must intervene")
                .tags(Tags.of(TAG_CAUSE, cause))
                .register(it)
                .increment()
        }
    }

    companion object {
        const val MESSAGES_READ = "cdc.outbox.messages.read"
        const val MESSAGES_PUBLISHED = "cdc.outbox.messages.published"
        const val MESSAGES_FAILED = "cdc.outbox.messages.failed"
        const val MESSAGES_DISCARDED = "cdc.outbox.messages.discarded"
        const val PUBLISH_DURATION = "cdc.outbox.publish.duration"
        const val PUBLISH_RETRIES = "cdc.outbox.publish.retries"
        const val RECONNECTS = "cdc.outbox.reconnect.attempts"
        const val DEAD_LETTERED = "cdc.outbox.messages.dead_lettered"
        const val DEAD_LETTER_FAILURES = "cdc.outbox.dead_letter.failures"

        const val TAG_SLOT = "slot"
        const val TAG_SINK = "sink"
        const val TAG_TOPIC = "topic"
        const val TAG_CAUSE = "cause"
        const val TAG_REASON = "reason"
        const val TAG_ATTEMPT = "attempt"

        /** Returns a no-op instance that records nothing. */
        fun noop(): CdcOutboxMetrics = CdcOutboxMetrics(null)
    }
}
