package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status
import java.time.Duration

/**
 * Spring Boot Actuator health indicator for the CDC-outbox producer.
 *
 * Decision matrix (highest precedence first):
 *  - `pendingFailureLsn` is set → `DOWN`. A publish has failed and the
 *    slot is holding back redelivery; this needs operator attention.
 *  - producer is not running (the streaming loop is not iterating) →
 *    `DOWN`.
 *  - producer has not flushed within [maxIdle] (e.g. the loop is stuck
 *    or Postgres stopped emitting) → `OUT_OF_SERVICE`.
 *  - otherwise → `UP`.
 *
 * Exposes the slot name, running flag, the pending-failure LSN (if any),
 * the lifecycle phase, and the time-since-last-activity as `details`, so
 * the Actuator endpoint surfaces actionable diagnostics.
 */
class CdcOutboxHealthIndicator(
    private val producer: SlotReaderMessageProducer,
    private val lifecycle: CdcOutboxLifecycle,
    private val maxIdle: Duration,
) : HealthIndicator {

    override fun health(): Health {
        val state = producer.snapshotState()
        val idleForMs = state.msSinceLastActivity
        val idleDisplay = if (idleForMs == Long.MAX_VALUE) "never" else "${idleForMs}ms"
        val builder = Health.Builder()
            .withDetail("slot", state.slot)
            .withDetail("running", state.running)
            .withDetail("lifecycleRunning", lifecycle.isRunning)
            .withDetail("pendingFailureLsn", state.pendingFailureLsn ?: "none")
            .withDetail("idleFor", idleDisplay)
            .withDetail("maxIdle", "${maxIdle.toMillis()}ms")

        return when {
            state.pendingFailureLsn != null ->
                builder
                    .status(Status.DOWN)
                    .withDetail("reason", "pending publish failure; slot will not advance until redelivery")
                    .build()

            !state.running ->
                builder
                    .status(Status.DOWN)
                    .withDetail("reason", "producer is not running")
                    .build()

            idleForMs != Long.MAX_VALUE && idleForMs > maxIdle.toMillis() ->
                builder
                    .status(Status.OUT_OF_SERVICE)
                    .withDetail(
                        "reason",
                        "no activity for ${idleForMs}ms (threshold ${maxIdle.toMillis()}ms)",
                    )
                    .build()

            else -> builder.up().build()
        }
    }
}
