package br.com.fltech.cdc.outbox.publisher.observability

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.function.Supplier

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
// TooManyFunctions: the facade intentionally carries one method per
// metric so consumers never see the registry directly. Splitting it
// would scatter the metric inventory across files.
@Suppress("TooManyFunctions")
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

    /**
     * Records a failure while parsing or handling an event off the
     * MySQL binlog. The listener thread keeps going (otherwise the
     * adapter would silently die); this counter is the only durable
     * signal that the parse failed.
     */
    fun recordBinlogParseError(cause: String) {
        registry?.let {
            Counter.builder(BINLOG_PARSE_ERRORS)
                .description("MySQL binlog events that raised on parse/handle in the listener thread")
                .tags(Tags.of(TAG_CAUSE, cause))
                .register(it)
                .increment()
        }
    }

    /**
     * Records that the binlog adapter fell back from named columns to
     * indexed `col0`/`col1`/… for a given table. Reasons include: no
     * DataSource configured, INFORMATION_SCHEMA returned nothing for
     * the table, or the lookup itself raised.
     */
    fun recordBinlogColumnResolutionFallback(table: String) {
        registry?.let {
            Counter.builder(BINLOG_COLUMN_RESOLUTION_FALLBACKS)
                .description("MySQL binlog rows emitted with indexed column names because name resolution failed")
                .tags(Tags.of(TAG_TABLE, table))
                .register(it)
                .increment()
        }
    }

    /**
     * Records that the file-backed checkpoint store touched an orphan
     * `<key>.json.tmp` left behind by a crash between `fsync` and the
     * final `ATOMIC_MOVE` of a prior `save`. The single-tag
     * `outcome` carries `deleted` (the file was reclaimed) or
     * `failed` (the cleanup itself raised IO). Operators alert on
     * either: even `deleted` is a signal that a previous crash got
     * close enough to a half-committed save to be worth a glance.
     */
    fun recordCheckpointOrphanSwept(outcome: String) {
        registry?.let {
            Counter.builder(CHECKPOINT_ORPHANS_SWEPT)
                .description("Orphan checkpoint .tmp files encountered at startup, by sweep outcome")
                .tags(Tags.of(TAG_OUTCOME, outcome))
                .register(it)
                .increment()
        }
    }

    /**
     * Registers a Micrometer [Gauge] reporting replication lag (in
     * bytes) for the source identified by [sourceLabel]. The [supplier]
     * is invoked by Micrometer on every scrape; implementations MUST
     * keep it cheap — typically backing it with an [java.util.concurrent.atomic.AtomicLong]
     * updated by a sampler on a slower cadence than the scrape interval.
     *
     * No-op when no [MeterRegistry] is wired, mirroring the rest of
     * the facade. Registration is idempotent at the Micrometer layer:
     * the registry deduplicates by (name + tags), so calling this
     * twice with the same `sourceLabel` returns the same meter rather
     * than failing.
     */
    fun registerLagGauge(sourceLabel: String, supplier: () -> Number) {
        registry?.let {
            Gauge.builder(SOURCE_LAG_BYTES, Supplier<Number> { supplier() })
                .description("Replication lag (bytes) between the source's persisted checkpoint and the upstream head")
                .tags(Tags.of(TAG_SOURCE, sourceLabel))
                .register(it)
        }
    }

    /**
     * Records an operator-driven DLQ replay action. `outcome`
     * differentiates `success`, `success_delete_failed` (publish
     * went through but the SQS delete did not — the message will
     * resurface and may be replayed twice), `publish_failed`,
     * `abandoned`, and `abandon_failed`. `sourceCause` is the
     * `failureType` field from the original DLQ envelope (the
     * exception class that caused the dead-letter); `targetScheme`
     * is the sink scheme the replay was directed at (which may
     * differ from the original via an operator override).
     */
    fun recordDlqReplay(outcome: String, sourceCause: String, targetScheme: String) {
        registry?.let {
            Counter.builder(DLQ_REPLAYS)
                .description("DLQ messages replayed back into the sink registry by operator action")
                .tags(
                    Tags.of(
                        TAG_OUTCOME, outcome,
                        TAG_SOURCE_CAUSE, sourceCause,
                        TAG_TARGET_SCHEME, targetScheme,
                    ),
                )
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
        const val BINLOG_PARSE_ERRORS = "cdc.outbox.source.binlog.parse_errors"
        const val BINLOG_COLUMN_RESOLUTION_FALLBACKS =
            "cdc.outbox.source.binlog.column_resolution.fallbacks"
        const val CHECKPOINT_ORPHANS_SWEPT = "cdc.outbox.checkpoint.orphans_swept"
        const val SOURCE_LAG_BYTES = "cdc.outbox.source.lag_bytes"
        const val DLQ_REPLAYS = "cdc.outbox.dlq.replays"

        const val TAG_SLOT = "slot"
        const val TAG_SINK = "sink"
        const val TAG_TOPIC = "topic"
        const val TAG_CAUSE = "cause"
        const val TAG_REASON = "reason"
        const val TAG_ATTEMPT = "attempt"
        const val TAG_TABLE = "table"
        const val TAG_OUTCOME = "outcome"
        const val TAG_SOURCE = "source"
        const val TAG_SOURCE_CAUSE = "source_cause"
        const val TAG_TARGET_SCHEME = "target_scheme"

        /** Returns a no-op instance that records nothing. */
        fun noop(): CdcOutboxMetrics = CdcOutboxMetrics(null)
    }
}
