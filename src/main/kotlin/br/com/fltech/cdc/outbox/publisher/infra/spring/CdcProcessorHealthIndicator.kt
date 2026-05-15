package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.core.application.CdcProcessor
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status

/**
 * Actuator [HealthIndicator] for the hexagonal orchestrator. Mirrors
 * the legacy [CdcOutboxHealthIndicator] but reads from a
 * [CdcProcessor] + [CdcProcessorLifecycle] pair so consumers who flip
 * to `processor.kind=hexagonal` keep getting an actionable
 * `/actuator/health` signal.
 *
 * Decision matrix:
 *  - lifecycle expected running but processor's loop isn't iterating → `DOWN`
 *  - both running → `UP`.
 *
 * Pending-failure / idle-time reporting from the legacy indicator are
 * intentionally left as a Wave 5.1 follow-up — `CdcProcessor` does
 * not yet expose those snapshots. Adding them is straightforward when
 * the binlog source and Postgres I/U/D source land their integration
 * tests (which is when operators will actually start consuming the
 * detail fields).
 */
class CdcProcessorHealthIndicator(
    private val processor: CdcProcessor,
    private val lifecycle: CdcProcessorLifecycle,
) : HealthIndicator {

    override fun health(): Health {
        val processorRunning = processor.isRunning()
        val lifecycleRunning = lifecycle.isRunning
        val builder = Health.Builder()
            .withDetail("processorRunning", processorRunning)
            .withDetail("lifecycleRunning", lifecycleRunning)

        return when {
            lifecycleRunning && !processorRunning ->
                builder
                    .status(Status.DOWN)
                    .withDetail("reason", "lifecycle is up but processor loop is not iterating")
                    .build()

            !lifecycleRunning ->
                builder
                    .status(Status.DOWN)
                    .withDetail("reason", "lifecycle has not started")
                    .build()

            else -> builder.up().build()
        }
    }
}
