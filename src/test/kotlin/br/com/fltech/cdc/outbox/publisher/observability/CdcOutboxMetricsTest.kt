package br.com.fltech.cdc.outbox.publisher.observability

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class CdcOutboxMetricsTest {

    @Test
    fun `null registry yields a no-op facade that does not throw`() {
        val metrics = CdcOutboxMetrics.noop()
        metrics.recordMessageRead("any_slot")
        metrics.recordPublished("sns", "topic-a")
        metrics.recordFailure("sqs", "queue-b", "RuntimeException")
        metrics.recordDiscarded("empty")
        metrics.recordPublishDuration("sns", Duration.ofMillis(42))
        metrics.recordReconnect("sql_exception")
    }

    @Test
    fun `counters increment under their canonical names with the expected tags`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)

        metrics.recordMessageRead("orders_outbox_slot")
        metrics.recordMessageRead("orders_outbox_slot")
        metrics.recordPublished(sink = "sns", topic = "orders.events")
        metrics.recordFailure(sink = "sqs", topic = "orders.events.dlq", cause = "TimeoutException")
        metrics.recordDiscarded(reason = "insert")

        assertEquals(
            2.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_READ,
                CdcOutboxMetrics.TAG_SLOT, "orders_outbox_slot",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_PUBLISHED,
                CdcOutboxMetrics.TAG_SINK, "sns",
                CdcOutboxMetrics.TAG_TOPIC, "orders.events",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_FAILED,
                CdcOutboxMetrics.TAG_SINK, "sqs",
                CdcOutboxMetrics.TAG_TOPIC, "orders.events.dlq",
                CdcOutboxMetrics.TAG_CAUSE, "TimeoutException",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.MESSAGES_DISCARDED,
                CdcOutboxMetrics.TAG_REASON, "insert",
            ).count(),
        )
    }

    @Test
    fun `publish duration is recorded under the sink tag`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)

        metrics.recordPublishDuration("kafka", Duration.ofMillis(123))
        metrics.recordPublishDuration("kafka", Duration.ofMillis(456))

        val timer = registry.timer(CdcOutboxMetrics.PUBLISH_DURATION, CdcOutboxMetrics.TAG_SINK, "kafka")
        assertNotNull(timer)
        assertEquals(2L, timer.count())
    }

    @Test
    fun `binlog parse error counter increments under the cause tag`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)

        metrics.recordBinlogParseError("IllegalStateException")
        metrics.recordBinlogParseError("IllegalStateException")
        metrics.recordBinlogParseError("NullPointerException")

        assertEquals(
            2.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_PARSE_ERRORS,
                CdcOutboxMetrics.TAG_CAUSE, "IllegalStateException",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_PARSE_ERRORS,
                CdcOutboxMetrics.TAG_CAUSE, "NullPointerException",
            ).count(),
        )
    }

    @Test
    fun `binlog column resolution fallback counter increments under the table tag`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)

        metrics.recordBinlogColumnResolutionFallback("public.orders")
        metrics.recordBinlogColumnResolutionFallback("public.orders")
        metrics.recordBinlogColumnResolutionFallback("public.customers")

        assertEquals(
            2.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_COLUMN_RESOLUTION_FALLBACKS,
                CdcOutboxMetrics.TAG_TABLE, "public.orders",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.BINLOG_COLUMN_RESOLUTION_FALLBACKS,
                CdcOutboxMetrics.TAG_TABLE, "public.customers",
            ).count(),
        )
    }

    @Test
    fun `binlog metrics no-op cleanly when no registry is wired`() {
        val metrics = CdcOutboxMetrics.noop()
        metrics.recordBinlogParseError("RuntimeException")
        metrics.recordBinlogColumnResolutionFallback("public.orders")
    }

    @Test
    fun `checkpoint orphan sweep counter increments under the outcome tag`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)

        metrics.recordCheckpointOrphanSwept("deleted")
        metrics.recordCheckpointOrphanSwept("deleted")
        metrics.recordCheckpointOrphanSwept("failed")

        assertEquals(
            2.0,
            registry.counter(
                CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT,
                CdcOutboxMetrics.TAG_OUTCOME, "deleted",
            ).count(),
        )
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT,
                CdcOutboxMetrics.TAG_OUTCOME, "failed",
            ).count(),
        )
    }

    @Test
    fun `checkpoint orphan sweep counter is a no-op without a registry`() {
        val metrics = CdcOutboxMetrics.noop()
        metrics.recordCheckpointOrphanSwept("deleted")
        metrics.recordCheckpointOrphanSwept("failed")
    }

    @Test
    fun `lag gauge is registered with the source tag and reads from the supplier`() {
        val registry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(registry)
        var current: Long = 1024
        metrics.registerLagGauge("postgres") { current }

        val gauge = registry.find(CdcOutboxMetrics.SOURCE_LAG_BYTES)
            .tag(CdcOutboxMetrics.TAG_SOURCE, "postgres")
            .gauge()
        assertNotNull(gauge)
        assertEquals(1024.0, gauge.value())

        // Updating the underlying source moves the gauge on next read —
        // proves the supplier is invoked lazily, not snapshotted at
        // registration time.
        current = 4096
        assertEquals(4096.0, gauge.value())
    }

    @Test
    fun `lag gauge registration is a no-op without a registry`() {
        val metrics = CdcOutboxMetrics.noop()
        // Must not throw — exercises the null-registry branch.
        metrics.registerLagGauge("mysql") { 0L }
    }
}
