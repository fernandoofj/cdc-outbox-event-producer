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
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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
     * Isolated in its own nested [Configuration], gated by
     * [ConditionalOnClass]: [FileCheckpointStore] lives in the
     * optional `:checkpoint-file` module, `compileOnly` in this
     * starter. Unlike the other nested configs in this file, this
     * bean's own signature (`CheckpointStore` in, `CdcOutboxProperties`/
     * `CdcOutboxMetrics` out — all `core`/local types) was never at
     * risk of the `Class.getDeclaredMethods()` crash the rest of this
     * round fixed; the risk here is narrower — the method BODY
     * constructs `FileCheckpointStore` unconditionally, so a consumer
     * who sets `cdc.outbox.checkpoint.enabled=true` without adding
     * `cdc-outbox-checkpoint-file` got a raw `NoClassDefFoundError` at
     * bean-creation time instead of a clean condition miss.
     *
     * `dlq-replay`'s auto-config uses the same `@ConditionalOnClass`
     * shape for its own optional dependency (Spring Security), but for
     * an opposite reason: not wiring there is a deliberate fail-*closed*
     * choice (a destructive action shouldn't ship without an auth
     * framework). Here, not wiring is fail-*open* — a real behavioral
     * loss for checkpoint-dependent sources, not a safety net — so it
     * pairs with [cdcOutboxCheckpointMisconfigurationGuard] below
     * instead of relying on the class gate alone.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(FileCheckpointStore::class)
    open class FileCheckpointStoreConfiguration {
        /**
         * Default [CheckpointStore] backed by [FileCheckpointStore], wired
         * only when `cdc.outbox.checkpoint.enabled=true`. Row-level
         * adapters that accept a checkpoint store (MySQL binlog,
         * Postgres WAL row source) take it as a constructor parameter
         * — the consumer wires it by hand when constructing the
         * adapter bean, this starter doesn't inject it for them.
         * Consumers can replace the wiring by supplying their own
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
        ): CheckpointStore = FileCheckpointStore(Paths.get(properties.checkpoint.directory), metrics)
    }

    /**
     * Safety net for the fail-open gap [FileCheckpointStoreConfiguration]
     * introduces: `cdc.outbox.checkpoint.enabled=true` no longer
     * guarantees a [CheckpointStore] gets wired (silently skipped when
     * `:checkpoint-file` is absent). No silent error paths per this
     * project's own rule — warn loudly and count it instead.
     *
     * Checked via `ObjectProvider`, evaluated at bean-*creation* time
     * — after every bean definition across every auto-config and user
     * config is already registered — so this reliably sees whichever
     * `CheckpointStore` ended up wired, from anywhere, regardless of
     * auto-configuration processing order. `.stream().count()`, not
     * `.ifAvailable`: the latter throws `NoUniqueBeanDefinitionException`
     * if a consumer happens to register two non-`@Primary`
     * `CheckpointStore` beans — a diagnostic bean must never itself
     * become a new way to fail startup.
     */
    @Bean
    @ConditionalOnProperty(
        prefix = "cdc.outbox.checkpoint",
        name = ["enabled"],
        havingValue = "true",
    )
    open fun cdcOutboxCheckpointMisconfigurationGuard(
        checkpointStoreProvider: ObjectProvider<CheckpointStore>,
        metrics: CdcOutboxMetrics,
    ): CheckpointConfigurationCheck {
        val misconfigured = checkpointStoreProvider.stream().count() == 0L
        if (misconfigured) {
            logger.warn(
                "cdc.outbox.checkpoint.enabled=true but no CheckpointStore was wired — " +
                    "is cdc-outbox-checkpoint-file on the classpath? Checkpoint-dependent " +
                    "sources (e.g. MySQL binlog) will resume from scratch on every restart.",
            )
            metrics.recordCheckpointMisconfigured()
        }
        return CheckpointConfigurationCheck(misconfigured)
    }

    /**
     * Result of [cdcOutboxCheckpointMisconfigurationGuard]'s check —
     * a real, inspectable value (not a fake marker) so a future health
     * indicator or test can read [misconfigured] directly instead of
     * relying solely on the log line and counter.
     */
    data class CheckpointConfigurationCheck(val misconfigured: Boolean)

    /**
     * Picks the right [CdcSource] given the consumer's wiring: a
     * [RowChangeSource] bean → wrap in [MappingCdcSource]. Consumers
     * opt in by registering a `RowChangeSource` (e.g. the MySQL binlog
     * or the Postgres WAL row source) instead of overriding the bare
     * `CdcSource` directly. `RowChangeSource`/`MappingRules` are
     * `core` types, always resolvable, so this method is safe to leave
     * directly on the outer class — unlike the Postgres fallback in
     * [PostgresSourceConfiguration] below, which needs `:source-postgres`
     * types in its signature.
     *
     * Mutual exclusion with [PostgresSourceConfiguration] is NOT a
     * registration-order race: Spring processes a configuration
     * class's nested member classes *before* its own `@Bean` methods
     * (`ConfigurationClassParser` calls `processMemberClasses` ahead of
     * bean-method retrieval), so the nested class always gets a chance
     * to register first regardless of textual order in this file. The
     * fallback instead checks `RowChangeSource` absence explicitly.
     *
     * That check is still a `@ConditionalOnBean`, evaluated at
     * bean-*definition* time as auto-configuration classes are
     * processed in order — unlike the pre-split code, which resolved
     * the row source via `ObjectProvider` at bean-*creation* time
     * (after every bean definition across every auto-config is
     * already registered, so registration order can't matter). The
     * consumer's [RowChangeSource] MUST come from a plain
     * `@Configuration`/`@Bean` (or a `@SpringBootApplication`-scanned
     * component) — those always register before
     * `@EnableAutoConfiguration`-imported classes, which is what every
     * adapter this project ships expects (README's Quick Start wires
     * `MySqlBinlogRowChangeSource`/`PgWalRowChangeSource` this way). A
     * `RowChangeSource` supplied by a THIRD-PARTY auto-configuration
     * with no explicit `@AutoConfigureBefore(CdcOutboxHexagonalAutoConfiguration::class)`
     * could lose this race and silently fall through to the Postgres
     * WAL default instead — with no error, log, or counter.
     */
    @Bean("cdcOutboxSource")
    @ConditionalOnBean(RowChangeSource::class)
    @ConditionalOnMissingBean(CdcSource::class)
    open fun cdcOutboxRowChangeSource(
        rowSource: RowChangeSource,
        mappingRulesProvider: ObjectProvider<MappingRules>,
    ): CdcSource {
        // ObjectProvider keeps the dependency on MappingRules *soft*:
        // when the consumer wires a row source but no rules, fall back
        // to MappingRules.EMPTY so the orchestrator stays consistent
        // with `CdcOutboxMappingAutoConfiguration`'s own default.
        val rules = mappingRulesProvider.getIfAvailable { MappingRules.EMPTY }
        return MappingCdcSource(rowSource, rules)
    }

    /**
     * Isolated in its own nested [Configuration], gated by
     * [ConditionalOnClass]: [PostgresConfiguration], [ReplicationConfiguration]
     * and [ConnectionProvider] live in the optional `:source-postgres`
     * module — a MySQL-only consumer (README's "Setup MySQL binlog +
     * Kafka") never declares it. This bean used to sit directly on the
     * outer class with these types as unconditional parameters, which
     * crashes `Class.getDeclaredMethods()` for the whole class the
     * moment `:source-postgres` is absent — the same failure mode this
     * file's [LegacyDeadLetterAdapterConfiguration] and
     * [LagProbeConfiguration] already guard against for their own
     * optional dependencies.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(PostgresConfiguration::class)
    open class PostgresSourceConfiguration {
        /**
         * `@ConditionalOnMissingBean(value = [CdcSource::class, RowChangeSource::class])`,
         * not just `CdcSource` alone: nested member classes are
         * processed before [cdcOutboxRowChangeSource] above, so relying
         * on "no `CdcSource` bean yet" would always be true at this
         * point and this fallback would win even when a
         * [RowChangeSource] is present. Checking `RowChangeSource`
         * absence directly makes the precedence correct regardless of
         * processing order.
         */
        @Bean("cdcOutboxSource")
        @ConditionalOnMissingBean(value = [CdcSource::class, RowChangeSource::class])
        open fun cdcOutboxPostgresSource(
            postgresConfiguration: PostgresConfiguration,
            replicationConfiguration: ReplicationConfiguration,
            connectionProvider: ConnectionProvider,
        ): CdcSource =
            PgLogicalReplicationCdcSource(
                postgresConfiguration = postgresConfiguration,
                replicationConfiguration = replicationConfiguration,
                connectionProvider = connectionProvider,
                objectMapper = defaultMapper,
            )
    }

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
    ): CdcProcessor =
        CdcProcessor(
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
     * Isolated in its own nested [Configuration], gated by
     * [ConditionalOnClass]: [LagProbeScheduler] (and [PostgresLagProbe]
     * / [MysqlLagProbe], used in the method bodies below) live in the
     * optional `:lag-probes` module, `compileOnly` in this starter and
     * a *separate* Maven coordinate a consumer must add explicitly
     * (see README § Tabela de coordenadas). `lag.enabled` defaults to
     * `true`, so without this isolation every minimal consumer who
     * skips `cdc-outbox-lag-probes` — e.g. the README's own "Setup
     * mínimo — Postgres → SNS" — would crash the whole hexagonal
     * auto-config: `cdcOutboxLagProbeScheduler`'s return type
     * ([LagProbeScheduler]) makes `Class.getDeclaredMethods()` throw
     * for the *enclosing* class the moment Spring introspects it for
     * any other `@Bean` method's condition, not just this one's.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(LagProbeScheduler::class)
    open class LagProbeConfiguration {
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
        ): LagProbe =
            PostgresLagProbe(
                postgresConfiguration = postgresConfiguration,
                connectionProvider = connectionProvider,
                slotName = properties.replication.slotName,
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
        ): LagProbeScheduler =
            LagProbeScheduler(
                probe = lagProbe,
                metrics = metrics,
                interval = properties.lag.interval,
            )

        /**
         * MySQL [LagProbe] — nested one level further. In practice
         * `:lag-probes` declares `api(project(":source-mysql"))`, so
         * [MySqlBinlogRowChangeSource] is always on the classpath
         * whenever [LagProbeScheduler] is; this [ConditionalOnClass] is
         * defensive rather than guarding an actually-reachable gap —
         * against a consumer-side Gradle `exclude` on the transitive
         * dependency, or a future `:lag-probes` split that drops the
         * `api` coupling. Cheap to keep, mirrors the same technique
         * used one level up in [LagProbeConfiguration].
         */
        @Configuration(proxyBeanMethods = false)
        @ConditionalOnClass(MySqlBinlogRowChangeSource::class)
        open class MysqlLagProbeConfiguration {
            /**
             * Wired only when the consumer registered a
             * [MySqlBinlogRowChangeSource] AND a [CheckpointStore] AND a
             * [DataSource]. The checkpoint store dependency is explicit
             * because the probe reads the `binlog:<serverId>` key the
             * binlog source persists on every ack — without it, lag is
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
            ): LagProbe =
                MysqlLagProbe(
                    dataSource = dataSource,
                    checkpointStore = checkpointStore,
                    // The binlog source's serverId is the source of truth
                    // for the checkpoint key; reading it back here keeps
                    // the two sides aligned without a second property knob.
                    serverId = mySqlBinlogRowChangeSource.serverId,
                )
        }
    }

    /**
     * Isolated in its own nested [Configuration], gated by
     * [ConditionalOnClass]: [DeadLetterSink] lives in the optional
     * `:legacy` module, `compileOnly` in this starter. A `@Bean`
     * method referencing it as a parameter type on the OUTER class
     * would crash every consumer who doesn't add `cdc-outbox-legacy`
     * — `Class.getDeclaredMethods()` resolves every method's
     * signature eagerly when Spring introspects a configuration
     * class for its OTHER `@Bean` methods, not just the ones whose
     * conditions evaluate true, so a single unresolvable parameter
     * type takes down the whole class with a `NoClassDefFoundError`.
     * `@ConditionalOnClass` on a nested class is checked from ASM
     * annotation metadata before the class is ever loaded, so the
     * nested class — and its [DeadLetterSink]-typed method — is
     * never touched when `:legacy` is absent.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(DeadLetterSink::class)
    open class LegacyDeadLetterAdapterConfiguration {
        @Bean
        @ConditionalOnBean(DeadLetterSink::class)
        @ConditionalOnMissingBean(DeadLetterPort::class)
        open fun cdcOutboxDeadLetterPort(legacy: DeadLetterSink): DeadLetterPort = LegacyDeadLetterPortAdapter(legacy)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CdcOutboxHexagonalAutoConfiguration::class.java)
    }
}
