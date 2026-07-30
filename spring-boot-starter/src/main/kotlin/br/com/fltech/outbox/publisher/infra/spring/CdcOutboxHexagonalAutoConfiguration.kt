package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.adapter.checkpoint.FileCheckpointStore
import br.com.fltech.outbox.publisher.adapter.deadletter.LegacyDeadLetterPortAdapter
import br.com.fltech.outbox.publisher.adapter.lag.mysql.MysqlLagProbe
import br.com.fltech.outbox.publisher.adapter.lag.postgres.PostgresLagProbe
import br.com.fltech.outbox.publisher.adapter.source.mysql.MySqlBinlogRowChangeSource
import br.com.fltech.outbox.publisher.adapter.source.postgres.PgLogicalReplicationCdcSource
import br.com.fltech.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource
import br.com.fltech.outbox.publisher.core.application.CdcProcessor
import br.com.fltech.outbox.publisher.core.application.MappingCdcSource
import br.com.fltech.outbox.publisher.core.port.CdcSource
import br.com.fltech.outbox.publisher.core.port.CheckpointStore
import br.com.fltech.outbox.publisher.core.port.DeadLetterPort
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.core.port.LagProbe
import br.com.fltech.outbox.publisher.core.port.MappingRules
import br.com.fltech.outbox.publisher.core.port.RowChangeSource
import br.com.fltech.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.outbox.publisher.observability.LagProbeScheduler
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.outbox.publisher.retry.BackOff
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import java.nio.file.Paths
import javax.sql.DataSource

/**
 * Wires the hexagonal orchestrator path:
 *  - `cdcOutboxSource`: a [CdcSource] bean. Resolution order:
 *      1. consumer-provided [CdcSource] bean — wins outright.
 *      2. consumer-provided [RowChangeSource] bean — gets wrapped in
 *         a [MappingCdcSource] together with the
 *         [MappingRules] from [CdcOutboxMappingAutoConfiguration].
 *         This is the path for the new Wave 5.2
 *         [br.com.fltech.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource]
 *         and for the Wave 5 MySQL binlog adapter.
 *      3. default — a [PgLogicalReplicationCdcSource] streaming
 *         `pg_logical_emit_message` records out of the configured
 *         slot.
 *  - `cdcOutboxProcessor`: a [CdcProcessor] driven by the source + the
 *    [EventSinkRegistry] from [CdcOutboxSinkAutoConfiguration].
 *  - `cdcOutboxProcessorLifecycle`: a [CdcProcessorLifecycle] running
 *    the processor on a daemon thread inside the Spring lifecycle.
 *
 * Activated only when `cdc.outbox.processor.kind=hexagonal`. The
 * legacy [br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer]
 * path in [CdcOutboxAutoConfiguration] is mutually exclusive — the
 * legacy lifecycle disables itself when `processor.kind=hexagonal` (see
 * its `@ConditionalOnProperty`).
 */
@AutoConfiguration
@AutoConfigureAfter(
    CdcOutboxAutoConfiguration::class,
    CdcOutboxSinkAutoConfiguration::class,
    CdcOutboxMappingAutoConfiguration::class,
)
@ConditionalOnProperty(
    prefix = "cdc.outbox.processor",
    name = ["kind"],
    havingValue = "hexagonal",
    // Flipped in Wave 5: `hexagonal` is now the default. If
    // `cdc.outbox.processor.kind` is unset, this autoconfig wires.
    // Set the property to `legacy` to keep the pre-Wave-5 chain.
    matchIfMissing = true,
)
open class CdcOutboxHexagonalAutoConfiguration {

    /**
     * Default [CheckpointStore] backed by [FileCheckpointStore], wired
     * only when `cdc.outbox.checkpoint.enabled=true`. Adapters that
     * accept a checkpoint store (MySQL binlog, Postgres WAL row
     * source) pick it up via [ObjectProvider] in the bean definitions
     * below. Consumers can replace the wiring by supplying their own
     * [CheckpointStore] bean — the standard
     * `@ConditionalOnMissingBean` rule applies.
     *
     * [CdcOutboxMetrics] is threaded in so the constructor-time
     * orphan-`.tmp` sweep can publish
     * `cdc.outbox.checkpoint.orphans_swept{outcome}` against the
     * application's Micrometer registry. Without this thread the
     * counter stays at the no-op facade and operators see no signal.
     */
    @Bean
    @ConditionalOnMissingBean(CheckpointStore::class)
    @ConditionalOnProperty(
        prefix = "cdc.outbox.checkpoint",
        name = ["enabled"],
        havingValue = "true",
    )
    open fun cdcOutboxCheckpointStore(
        properties: CdcOutboxProperties,
        metrics: CdcOutboxMetrics,
    ): CheckpointStore =
        FileCheckpointStore(Paths.get(properties.checkpoint.directory), metrics)

    /**
     * Picks the right [CdcSource] given the consumer's wiring:
     *  - a [RowChangeSource] bean → wrap in [MappingCdcSource].
     *    Consumers opt in by registering a `RowChangeSource` (e.g.
     *    the MySQL binlog or the new Postgres WAL row source) instead
     *    of overriding the bare `CdcSource` directly.
     *  - nothing provided → fall through to the Postgres
     *    message-flow default.
     *
     * The `@ConditionalOnMissingBean(CdcSource::class)` keeps the
     * consumer-supplied path always winning over either branch.
     */
    @Bean("cdcOutboxSource")
    @ConditionalOnMissingBean(CdcSource::class)
    open fun cdcOutboxSource(
        postgresConfiguration: PostgresConfiguration,
        replicationConfiguration: ReplicationConfiguration,
        connectionProvider: ConnectionProvider,
        rowSourceProvider: ObjectProvider<RowChangeSource>,
        mappingRulesProvider: ObjectProvider<MappingRules>,
    ): CdcSource {
        val rowSource = rowSourceProvider.ifAvailable
        if (rowSource != null) {
            // ObjectProvider keeps the dependency on MappingRules
            // *soft*: when the consumer wires a row source but no
            // rules, fall back to MappingRules.EMPTY so the
            // orchestrator stays consistent with
            // `CdcOutboxMappingAutoConfiguration`'s own default.
            val rules = mappingRulesProvider.getIfAvailable { MappingRules.EMPTY }
            return MappingCdcSource(rowSource, rules)
        }
        return PgLogicalReplicationCdcSource(
            postgresConfiguration = postgresConfiguration,
            replicationConfiguration = replicationConfiguration,
            connectionProvider = connectionProvider,
            objectMapper = defaultMapper,
        )
    }

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

    // LongParameterList: same rationale as the legacy factory — Spring
    // wires collaborators by type, not by aggregate; a wrapper object
    // would just be ceremony.
    @Suppress("LongParameterList")
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

    /**
     * Postgres [LagProbe] — wired only when the consumer registered
     * a [PgWalRowChangeSource]. Reuses the shared
     * [ConnectionProvider] / [PostgresConfiguration] so the SQL hits
     * the same query-mode pool as the rest of the chain.
     *
     * Mutually exclusive with the MySQL probe at the
     * `@ConditionalOnBean(LagProbe::class)` level: whichever runs
     * first wins, and a consumer who needs both should wire their
     * own probe bean.
     */
    @Bean("cdcOutboxPostgresLagProbe")
    @ConditionalOnProperty(
        prefix = "cdc.outbox.lag",
        name = ["enabled"],
        havingValue = "true",
        matchIfMissing = true,
    )
    @ConditionalOnBean(PgWalRowChangeSource::class)
    @ConditionalOnMissingBean(LagProbe::class)
    open fun cdcOutboxPostgresLagProbe(
        postgresConfiguration: PostgresConfiguration,
        connectionProvider: ConnectionProvider,
        properties: CdcOutboxProperties,
    ): LagProbe = PostgresLagProbe(
        postgresConfiguration = postgresConfiguration,
        connectionProvider = connectionProvider,
        slotName = properties.replication.slotName,
    )

    /**
     * MySQL [LagProbe] — wired only when the consumer registered a
     * [MySqlBinlogRowChangeSource] AND a [CheckpointStore] AND a
     * [DataSource]. The checkpoint store dependency is explicit
     * because the probe reads the `binlog:<serverId>` key the binlog
     * source persists on every ack — without it, lag is
     * unrecoverable from the consumer side and the probe would
     * always return `null`.
     */
    @Bean("cdcOutboxMysqlLagProbe")
    @ConditionalOnProperty(
        prefix = "cdc.outbox.lag",
        name = ["enabled"],
        havingValue = "true",
        matchIfMissing = true,
    )
    @ConditionalOnBean(value = [MySqlBinlogRowChangeSource::class, CheckpointStore::class, DataSource::class])
    @ConditionalOnMissingBean(LagProbe::class)
    open fun cdcOutboxMysqlLagProbe(
        dataSource: DataSource,
        checkpointStore: CheckpointStore,
        mySqlBinlogRowChangeSource: MySqlBinlogRowChangeSource,
    ): LagProbe = MysqlLagProbe(
        dataSource = dataSource,
        checkpointStore = checkpointStore,
        // The binlog source's serverId is the source of truth for
        // the checkpoint key; reading it back here keeps the two
        // sides aligned without a second property knob.
        serverId = mySqlBinlogRowChangeSource.serverId,
    )

    /**
     * Background sampler that feeds the `cdc.outbox.source.lag_bytes`
     * Micrometer gauge. Started in `init-method` and stopped in
     * `destroy-method` so it tracks the Spring context lifecycle
     * without needing to extend `SmartLifecycle` — the probe is
     * passive and has no ordering requirement against the
     * orchestrator.
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(
        prefix = "cdc.outbox.lag",
        name = ["enabled"],
        havingValue = "true",
        matchIfMissing = true,
    )
    @ConditionalOnBean(LagProbe::class)
    @ConditionalOnMissingBean(LagProbeScheduler::class)
    open fun cdcOutboxLagProbeScheduler(
        lagProbe: LagProbe,
        metrics: CdcOutboxMetrics,
        properties: CdcOutboxProperties,
    ): LagProbeScheduler = LagProbeScheduler(
        probe = lagProbe,
        metrics = metrics,
        interval = properties.lag.interval,
    )
}
