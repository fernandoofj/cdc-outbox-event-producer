package br.com.fltech.outbox.publisher.adapter.dlq.replay

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * Orchestrates the read → publish → delete loop for DLQ replay.
 *
 * Operations are exposed at the [DlqReplayActuatorEndpoint] layer
 * (HTTP / JMX) but kept here free of any web concern so the same
 * service is reusable from a future CLI or unit test driver.
 *
 * At-least-once contract: a `publish` succeeded but a subsequent
 * `delete` failure leaves the message visible again on the DLQ.
 * The operator (or a retry) may then replay it twice. Consumer
 * idempotency is assumed — same guarantee the producer makes for
 * the happy path. The [ReplayOutcome] distinguishes these cases so
 * operators see exactly what happened.
 */
class DlqReplayService(
    private val reader: DlqReader,
    private val sinkRegistry: EventSinkRegistry,
    private val metrics: CdcOutboxMetrics,
) {

    /** Lists up to [max] messages without removing them from the DLQ. */
    fun peek(max: Int): List<PeekedMessage> {
        val messages = reader.peek(max.coerceIn(1, PEEK_CAP))
        return messages.map { msg ->
            PeekedMessage(
                handle = msg.handle.value,
                envelope = msg.envelope,
            )
        }
    }

    /** Aggregate stats for operator visibility. */
    fun stats(): DlqReader.Stats = reader.stats()

    /**
     * Replays a single message. Envelope is taken from the caller
     * (typically returned by a prior [peek]) so the service does
     * not need to re-fetch by handle. Override allows routing the
     * replay to a different scheme/target when the original sink
     * has been migrated.
     */
    fun replay(handle: String, envelope: DlqEnvelope, override: RoutingOverride? = null): ReplayOutcome {
        val routing = resolveRouting(envelope, override)
        val event = buildEvent(envelope, routing)
        return try {
            sinkRegistry.publish(routing, event)
            val deleteOutcome = tryDelete(handle, routing)
            val outcome = if (deleteOutcome) {
                ReplayOutcome.Success(routing.scheme)
            } else {
                ReplayOutcome.SuccessButDeleteFailed(routing.scheme)
            }
            metrics.recordDlqReplay(outcome.metricLabel, envelope.failureType, routing.scheme)
            logger.info(
                "DlqReplayService: replay action=replay handle={} lsn={} source_scheme={} target_scheme={} outcome={}",
                handle, envelope.lsn, parseOriginalScheme(envelope), routing.scheme, outcome.metricLabel,
            )
            outcome
        } catch (e: Exception) {
            // Publish failed — leave the message in the DLQ for
            // another attempt rather than double-DLQ'ing it.
            metrics.recordDlqReplay("publish_failed", envelope.failureType, routing.scheme)
            logger.error(
                "DlqReplayService: replay action=replay handle={} lsn={} target_scheme={} " +
                    "outcome=publish_failed cause={}",
                handle, envelope.lsn, routing.scheme, e.javaClass.simpleName, e,
            )
            ReplayOutcome.PublishFailed(routing.scheme, e.javaClass.simpleName, e.message)
        }
    }

    /**
     * Peeks up to [max] messages and replays them in sequence. When
     * [dryRun] is true the messages are not published and not
     * deleted — useful to preview what a replay would do without
     * touching downstream sinks.
     */
    fun replayBulk(max: Int, dryRun: Boolean): BulkReplayResult {
        val messages = reader.peek(max.coerceIn(1, PEEK_CAP))
        if (dryRun) {
            logger.info(
                "DlqReplayService: bulk replay dry_run=true count={} (no publish, no delete performed)",
                messages.size,
            )
            return BulkReplayResult(
                requested = max,
                attempted = messages.size,
                succeeded = 0,
                failed = 0,
                dryRun = true,
                previews = messages.map { PeekedMessage(it.handle.value, it.envelope) },
            )
        }
        var succeeded = 0
        var failed = 0
        messages.forEach { msg ->
            when (replay(msg.handle.value, msg.envelope, override = null)) {
                is ReplayOutcome.Success, is ReplayOutcome.SuccessButDeleteFailed -> succeeded += 1
                is ReplayOutcome.PublishFailed -> failed += 1
            }
        }
        logger.info(
            "DlqReplayService: bulk replay count={} success={} failed={} dry_run=false",
            messages.size, succeeded, failed,
        )
        return BulkReplayResult(
            requested = max,
            attempted = messages.size,
            succeeded = succeeded,
            failed = failed,
            dryRun = false,
            previews = emptyList(),
        )
    }

    /** Permanently removes a message without replaying it. */
    fun abandon(handle: String): AbandonOutcome {
        return try {
            reader.delete(DlqReader.Handle(handle))
            metrics.recordDlqReplay("abandoned", "manual", "none")
            logger.info("DlqReplayService: replay action=abandon handle={} outcome=success", handle)
            AbandonOutcome.Success
        } catch (e: Exception) {
            metrics.recordDlqReplay("abandon_failed", "manual", "none")
            logger.error(
                "DlqReplayService: replay action=abandon handle={} outcome=failed cause={}",
                handle, e.javaClass.simpleName, e,
            )
            AbandonOutcome.Failed(e.javaClass.simpleName, e.message)
        }
    }

    private fun resolveRouting(envelope: DlqEnvelope, override: RoutingOverride?): Routing {
        val original = Routing.parsePrefix(envelope.originalPrefix)
        return if (override == null) {
            original
        } else {
            Routing(scheme = override.scheme, target = override.target, attributes = original.attributes)
        }
    }

    private fun buildEvent(envelope: DlqEnvelope, routing: Routing): OutboxEvent {
        val occurredAt = try {
            Instant.parse(envelope.deadLetteredAt)
        } catch (e: DateTimeParseException) {
            // Fall back to now() rather than rejecting the replay —
            // the dead-lettered-at field is diagnostic, not part of
            // the published payload's identity.
            logger.warn(
                "DlqReplayService: deadLetteredAt='{}' did not parse; using current time. Cause={}",
                envelope.deadLetteredAt, e.javaClass.simpleName, e,
            )
            Instant.now()
        }
        return OutboxEvent(
            id = envelope.lsn,
            routing = routing,
            payload = envelope.content.toByteArray(Charsets.UTF_8),
            occurredAt = occurredAt,
            sourceCheckpoint = envelope.lsn,
        )
    }

    private fun tryDelete(handle: String, routing: Routing): Boolean = try {
        reader.delete(DlqReader.Handle(handle))
        true
    } catch (e: Exception) {
        logger.warn(
            "DlqReplayService: publish succeeded but delete failed handle={} target_scheme={} cause={}. " +
                "Message will become visible again; manual cleanup required to avoid double-publish.",
            handle, routing.scheme, e.javaClass.simpleName, e,
        )
        false
    }

    private fun parseOriginalScheme(envelope: DlqEnvelope): String =
        try {
            Routing.parsePrefix(envelope.originalPrefix).scheme
        } catch (e: Exception) {
            // Don't crash logging on a malformed prefix.
            "unknown:${e.javaClass.simpleName}"
        }

    data class PeekedMessage(val handle: String, val envelope: DlqEnvelope)

    data class RoutingOverride(val scheme: String, val target: String)

    sealed class ReplayOutcome(val metricLabel: String) {
        data class Success(val scheme: String) : ReplayOutcome("success")
        data class SuccessButDeleteFailed(val scheme: String) : ReplayOutcome("success_delete_failed")
        data class PublishFailed(val scheme: String, val cause: String, val message: String?) :
            ReplayOutcome("publish_failed")
    }

    sealed class AbandonOutcome {
        data object Success : AbandonOutcome()
        data class Failed(val cause: String, val message: String?) : AbandonOutcome()
    }

    data class BulkReplayResult(
        val requested: Int,
        val attempted: Int,
        val succeeded: Int,
        val failed: Int,
        val dryRun: Boolean,
        val previews: List<PeekedMessage>,
    )

    companion object {
        private val logger = LoggerFactory.getLogger(DlqReplayService::class.java)
        private const val PEEK_CAP = 10
    }
}
