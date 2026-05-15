package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.adapter.dlq.replay.DlqReader
import br.com.fltech.cdc.outbox.publisher.adapter.dlq.replay.DlqReplayActuatorEndpoint
import br.com.fltech.cdc.outbox.publisher.adapter.dlq.replay.DlqReplayService
import br.com.fltech.cdc.outbox.publisher.adapter.dlq.replay.SqsDlqReader
import br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import software.amazon.awssdk.services.sqs.SqsClient

/**
 * Wires the DLQ replay endpoint at `/actuator/cdcOutboxDlq`.
 *
 * Defence-in-depth on authentication:
 *
 *  1. `@ConditionalOnClass(SecurityFilterChain)` — if the consumer
 *     does not bring Spring Security on their classpath the
 *     auto-config refuses to wire the endpoint entirely.
 *     The DLQ replay action is destructive (re-injects events into
 *     downstream sinks); shipping it without an auth framework
 *     would be a foot-gun.
 *  2. Inside [DlqReplayActuatorEndpoint] each operation method
 *     re-asserts that the current `SecurityContext` carries an
 *     authenticated (non-anonymous) principal. If the consumer
 *     misconfigured their filter chain to permit anonymous access
 *     to `/actuator/cdcOutboxDlq`, the runtime check still
 *     refuses the request.
 *
 * Wired only when `cdc.outbox.dlq.replay.enabled=true` — opt-in by
 * design.
 */
@AutoConfiguration
@AutoConfigureAfter(CdcOutboxSinkAutoConfiguration::class)
@ConditionalOnProperty(
    prefix = "cdc.outbox.dlq.replay",
    name = ["enabled"],
    havingValue = "true",
)
@ConditionalOnClass(
    name = [
        "org.springframework.security.web.SecurityFilterChain",
        "software.amazon.awssdk.services.sqs.SqsClient",
        "org.springframework.boot.actuate.endpoint.annotation.Endpoint",
    ],
)
@ConditionalOnBean(EventSinkRegistry::class)
open class CdcOutboxDlqReplayAutoConfiguration {

    /**
     * Default [DlqReader] backed by SQS. Reads the same queue the
     * `SqsDeadLetterSink` writes to. Consumers can replace this
     * bean to point at a different DLQ backend (Kafka, Rabbit) —
     * the standard `@ConditionalOnMissingBean` rule applies.
     */
    @Bean
    @ConditionalOnMissingBean(DlqReader::class)
    @ConditionalOnBean(SqsClient::class)
    open fun cdcOutboxDlqReader(
        sqsClient: SqsClient,
        properties: CdcOutboxProperties,
    ): DlqReader {
        val queueName = properties.dlq.replay.queueName
        require(queueName.isNotBlank()) {
            "cdc.outbox.dlq.replay.queue-name must be set when cdc.outbox.dlq.replay.enabled=true"
        }
        logger.info("CdcOutboxDlqReplayAutoConfiguration: wiring SqsDlqReader for queue '{}'", queueName)
        return SqsDlqReader(
            sqs = sqsClient,
            queueName = queueName,
            peekVisibilityTimeoutSeconds = properties.dlq.replay.peekVisibilityTimeoutSeconds,
        )
    }

    @Bean
    @ConditionalOnMissingBean(DlqReplayService::class)
    open fun cdcOutboxDlqReplayService(
        reader: DlqReader,
        sinkRegistry: EventSinkRegistry,
        metrics: CdcOutboxMetrics,
    ): DlqReplayService = DlqReplayService(reader, sinkRegistry, metrics)

    @Bean
    @ConditionalOnMissingBean(DlqReplayActuatorEndpoint::class)
    open fun cdcOutboxDlqReplayEndpoint(service: DlqReplayService): DlqReplayActuatorEndpoint =
        DlqReplayActuatorEndpoint(service)

    companion object {
        private val logger = LoggerFactory.getLogger(CdcOutboxDlqReplayAutoConfiguration::class.java)
    }
}
