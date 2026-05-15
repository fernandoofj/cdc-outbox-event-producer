package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.core.application.CdcProcessor
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status
import java.time.Duration

/**
 * Actuator [HealthIndicator] for the hexagonal orchestrator. Mirrors
 * the legacy [CdcOutboxHealthIndicator] but reads from a
 * [CdcProcessor] + [CdcProcessorLifecycle] pair so consumers who flip
 * to `processor.kind=hexagonal` keep getting an actionable
 * `/actuator/health` signal.
 *
 * Decision matrix (highest precedence first):
 *  - `pendingFailureCheckpoint` is set → `DOWN`. A publish has
 *    failed and the source has not been acked yet; the orchestrator
 *    is either still retrying the same event or stuck after
 *    exhausting retries without a DLQ. Either way an operator must
 *    look. Same precedence as the legacy indicator's
 *    `pendingFailureLsn` branch.
 *  - lifecycle has not started → `DOWN`. The Spring context didn't
 *    flip the lifecycle on; the orchestrator was never asked to run.
 *  - lifecycle is running but the orchestrator's `running` flag is
 *    false → `DOWN`. The loop exited unexpectedly (e.g. shutdown
 *    raced with a publish failure, an uncaught error inside the
 *    daemon thread).
 *  - lifecycle and orchestrator both running but the loop has not
 *    iterated within [maxIdle] → `OUT_OF_SERVICE`. The loop is stuck
 *    upstream (source not producing, JDBC connection wedged, …) — a
 *    finite signal that operators should investigate.
 *  - otherwise → `UP`.
 */
class CdcProcessorHealthIndicator(
    private val processor: CdcProcessor,
    private val lifecycle: CdcProcessorLifecycle,
    private val maxIdle: Duration,
) : HealthIndicator {

    override fun health(): Health {
        val state = processor.snapshotState()
        val lifecycleRunning = lifecycle.isRunning
        val idleForMs = state.msSinceLastActivity
        val idleDisplay = if (idleForMs == Long.MAX_VALUE) "never" else "${idleForMs}ms"
        val builder = Health.Builder()
            .withDetail("slot", state.slot)
            .withDetail("processorRunning", state.running)
            .withDetail("lifecycleRunning", lifecycleRunning)
            .withDetail("pendingFailureCheckpoint", state.pendingFailureCheckpoint ?: "none")
            .withDetail("idleFor", idleDisplay)
            .withDetail("maxIdle", "${maxIdle.toMillis()}ms")

        return when {
            state.pendingFailureCheckpoint != null ->
                builder
                    .status(Status.DOWN)
                    .withDetail(
                        "reason",
                        "pending publish failure; source will not be acked until redelivery or dead-letter",
                    )
                    .build()

            !lifecycleRunning ->
                builder
                    .status(Status.DOWN)
                    .withDetail("reason", "lifecycle has not started")
                    .build()

            !state.running ->
                builder
                    .status(Status.DOWN)
                    .withDetail("reason", "lifecycle is up but processor loop is not iterating")
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
