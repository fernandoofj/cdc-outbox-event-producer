package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.adapter.deadletter.LegacyDeadLetterPortAdapter
import br.com.fltech.cdc.outbox.publisher.adapter.source.postgres.PgLogicalReplicationCdcSource
import br.com.fltech.cdc.outbox.publisher.core.application.CdcProcessor
import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource
import br.com.fltech.cdc.outbox.publisher.core.port.DeadLetterPort
import br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.cdc.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.cdc.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.cdc.outbox.publisher.retry.BackOff
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean

/**
 * Wires the hexagonal orchestrator path:
 *  - `cdcOutboxSource`: a [PgLogicalReplicationCdcSource] (default).
 *    Consumers can override by providing their own `CdcSource` bean
 *    (MySQL, SQL Server, Oracle …).
 *  - `cdcOutboxProcessor`: a [CdcProcessor] driven by the source + the
 *    [EventSinkRegistry] from [CdcOutboxSinkAutoConfiguration].
 *  - `cdcOutboxProcessorLifecycle`: a [CdcProcessorLifecycle] running
 *    the processor on a daemon thread inside the Spring lifecycle.
 *
 * Activated only when `cdc.outbox.processor.kind=hexagonal`. The
 * legacy [br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer]
 * path in [CdcOutboxAutoConfiguration] is mutually exclusive — the
 * legacy lifecycle disables itself when `processor.kind=hexagonal` (see
 * its `@ConditionalOnProperty`).
 */
@AutoConfiguration
@AutoConfigureAfter(CdcOutboxAutoConfiguration::class, CdcOutboxSinkAutoConfiguration::class)
@ConditionalOnProperty(
    prefix = "cdc.outbox.processor",
    name = ["kind"],
    havingValue = "hexagonal",
)
open class CdcOutboxHexagonalAutoConfiguration {

    @Bean("cdcOutboxSource")
    @ConditionalOnMissingBean(CdcSource::class)
    open fun cdcOutboxSource(
        postgresConfiguration: PostgresConfiguration,
        replicationConfiguration: ReplicationConfiguration,
        connectionProvider: ConnectionProvider,
    ): CdcSource = PgLogicalReplicationCdcSource(
        postgresConfiguration = postgresConfiguration,
        replicationConfiguration = replicationConfiguration,
        connectionProvider = connectionProvider,
        objectMapper = defaultMapper,
    )

    /**
     * Adapts a legacy [DeadLetterSink] bean to the hexagonal
     * [DeadLetterPort]. Only wired when a [DeadLetterSink] is present
     * AND no `DeadLetterPort` bean already exists — the consumer can
     * supply their own `DeadLetterPort` directly and it takes
     * precedence.
     */
    @Bean
    @ConditionalOnBean(DeadLetterSink::class)
    @ConditionalOnMissingBean(DeadLetterPort::class)
    open fun cdcOutboxDeadLetterPort(legacy: DeadLetterSink): DeadLetterPort =
        LegacyDeadLetterPortAdapter(legacy)

    @Bean("cdcOutboxProcessor")
    @ConditionalOnBean(value = [CdcSource::class, EventSinkRegistry::class])
    @ConditionalOnMissingBean(CdcProcessor::class)
    open fun cdcOutboxProcessor(
        cdcOutboxSource: CdcSource,
        sinkRegistry: EventSinkRegistry,
        metrics: CdcOutboxMetrics,
        @Qualifier("cdcOutboxPublishBackOff") publishBackOff: BackOff,
        deadLetterPort: DeadLetterPort?,
        properties: CdcOutboxProperties,
    ): CdcProcessor = CdcProcessor(
        source = cdcOutboxSource,
        sinkRegistry = sinkRegistry,
        metrics = metrics,
        deadLetterPort = deadLetterPort,
        maxPublishAttempts = properties.retry.maxPublishAttempts,
        publishBackOff = publishBackOff,
        slotLabel = properties.replication.slotName,
    )

    @Bean("cdcOutboxProcessorLifecycle")
    @ConditionalOnBean(CdcProcessor::class)
    @ConditionalOnMissingBean(CdcProcessorLifecycle::class)
    open fun cdcOutboxProcessorLifecycle(cdcOutboxProcessor: CdcProcessor): CdcProcessorLifecycle =
        CdcProcessorLifecycle(cdcOutboxProcessor)
}
