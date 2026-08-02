package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.outbox.publisher.deadletter.SqsDeadLetterSink
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.outbox.publisher.replication.connector.HikariCPConnectionProvider
import br.com.fltech.outbox.publisher.retry.BackOff
import br.com.fltech.outbox.publisher.retry.ExponentialBackOff
import br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer
import io.awspring.cloud.sqs.operations.SqsTemplate
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Wires the beans shared by BOTH processor chains into a Spring Boot
 * application: [BackOff] / [CdcOutboxMetrics] unconditionally, plus
 * [PostgresConfiguration] / [ReplicationConfiguration] /
 * [ConnectionProvider] when `:source-postgres` is on the classpath (see
 * the nested [PostgresConnectionConfiguration]). Gated only by
 * `cdc.outbox.enabled` (default `true`) — NOT by the presence of
 * [SlotReaderMessageProducer], because [CdcOutboxHexagonalAutoConfiguration]
 * (the *default* processor since Wave 5) depends on these same beans and
 * must work without `:legacy` on the classpath. Every bean uses
 * `@ConditionalOnMissingBean` so a consumer can override any single piece
 * (e.g. inject a custom [ConnectionProvider] for tests) without disabling
 * the rest of the chain.
 *
 * Beans:
 *  - [PostgresConfiguration] / [ReplicationConfiguration]: derived from the
 *    `cdc.outbox.postgres.*` and `cdc.outbox.replication.*` property
 *    groups.
 *  - [ConnectionProvider]: a [HikariCPConnectionProvider] sized from
 *    `cdc.outbox.pool.*`.
 *  - [BackOff]: an [ExponentialBackOff] from `cdc.outbox.retry.*`.
 *  - [CdcOutboxMetrics]: bound to the application's [MeterRegistry] when
 *    one is present, no-op otherwise.
 *
 * The legacy-only beans ([SlotReaderMessageProducer], its [DeadLetterSink],
 * [CdcOutboxLifecycle]) live in the nested [LegacyProducerConfiguration],
 * gated by [ConditionalOnClass] on [SlotReaderMessageProducer] — isolating
 * them keeps this outer class safe to introspect when `:legacy` is absent
 * (see the equivalent fix in [CdcOutboxHexagonalAutoConfiguration] for why
 * an unresolvable method signature on this class would otherwise crash
 * `Class.getDeclaredMethods()` for every other bean method here too).
 *
 * [CdcOutboxHealthIndicator] is contributed by [CdcOutboxHealthAutoConfiguration]
 * — split into its own file for the same classloading reason (Boot 4 moved
 * `HealthIndicator` out of `spring-boot-actuator` into `spring-boot-health`).
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "cdc.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CdcOutboxProperties::class)
open class CdcOutboxAutoConfiguration {
    /**
     * [PostgresConfiguration], [ReplicationConfiguration] and
     * [ConnectionProvider] ([HikariCPConnectionProvider]) all live in
     * the optional `:source-postgres` module, `compileOnly` in this
     * starter — a MySQL-only consumer (README's "Setup MySQL binlog +
     * Kafka") never declares it. These three `@Bean` methods used to
     * sit directly on the outer class; that reintroduces the exact
     * `Class.getDeclaredMethods()` crash this file's own KDoc warns
     * about, just against `:source-postgres` instead of `:legacy`.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(PostgresConfiguration::class)
    open class PostgresConnectionConfiguration {
        @Bean
        @ConditionalOnMissingBean
        open fun cdcOutboxPostgresConfiguration(properties: CdcOutboxProperties): PostgresConfiguration =
            PostgresConfiguration(
                host = properties.postgres.host,
                port = properties.postgres.port,
                database = properties.postgres.database,
                username = properties.postgres.username,
                password = properties.postgres.password,
                sslMode = properties.postgres.sslMode,
                pathToRootCert = properties.postgres.pathToRootCert,
                sslPassword = properties.postgres.sslPassword,
                pathToSslKey = properties.postgres.pathToSslKey,
                pathToSslCert = properties.postgres.pathToSslCert,
            )

        @Bean
        @ConditionalOnMissingBean
        open fun cdcOutboxReplicationConfiguration(properties: CdcOutboxProperties): ReplicationConfiguration =
            ReplicationConfiguration(
                slotName = properties.replication.slotName,
                outputPlugin = properties.replication.outputPlugin,
                statusIntervalValue = properties.replication.statusInterval.toSeconds().toInt(),
                updateIdleSlotInterval = properties.replication.updateIdleSlotInterval.toSeconds(),
                existingProcessRetryLimit = properties.replication.existingProcessRetryLimit,
                existingProcessRetrySleepSeconds = properties.replication.existingProcessRetrySleep?.toSeconds(),
                includeXids = properties.replication.includeXids,
                includeLsn = properties.replication.includeLsn,
                formatVersion = properties.replication.formatVersion,
            )

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        open fun cdcOutboxConnectionProvider(properties: CdcOutboxProperties): ConnectionProvider =
            HikariCPConnectionProvider(
                HikariCPConnectionProvider.PoolConfig(
                    maximumPoolSize = properties.pool.maximumPoolSize,
                    minimumIdle = properties.pool.minimumIdle,
                    connectionTimeout = properties.pool.connectionTimeout,
                    validationTimeout = properties.pool.validationTimeout,
                    idleTimeout = properties.pool.idleTimeout,
                    maxLifetime = properties.pool.maxLifetime,
                    leakDetectionThreshold = properties.pool.leakDetectionThreshold,
                    poolName = properties.pool.poolName,
                    keepaliveTime = properties.pool.keepaliveTime,
                    autoCommit = properties.pool.autoCommit,
                ),
            )
    }

    @Bean
    @ConditionalOnMissingBean(name = ["cdcOutboxReconnectBackOff"])
    open fun cdcOutboxReconnectBackOff(properties: CdcOutboxProperties): BackOff =
        ExponentialBackOff(
            initial = properties.retry.initial,
            max = properties.retry.max,
            multiplier = properties.retry.multiplier,
            jitter = properties.retry.jitter,
        )

    @Bean
    @ConditionalOnMissingBean
    open fun cdcOutboxMetrics(meterRegistry: MeterRegistry?): CdcOutboxMetrics = CdcOutboxMetrics(meterRegistry)

    @Bean("cdcOutboxPublishBackOff")
    @ConditionalOnMissingBean(name = ["cdcOutboxPublishBackOff"])
    open fun cdcOutboxPublishBackOff(properties: CdcOutboxProperties): BackOff =
        ExponentialBackOff(
            initial = properties.retry.publishBackoffInitial,
            max = properties.retry.publishBackoffMax,
            multiplier = properties.retry.multiplier,
            jitter = properties.retry.jitter,
        )

    /**
     * Isolated in its own nested [Configuration], gated by
     * [ConditionalOnClass]: [SlotReaderMessageProducer], [SNSProducer],
     * [SQSProducer] and [DeadLetterSink] all live in the optional
     * `:legacy` module, `compileOnly` in this starter. Before this fix
     * these three `@Bean` methods lived directly on the OUTER class —
     * which also gated the whole outer class behind
     * `@ConditionalOnClass(SlotReaderMessageProducer::class)`, taking
     * the shared beans above down with it for every consumer using the
     * *default* hexagonal chain without `:legacy`. Nesting narrows the
     * blast radius to exactly the legacy-only beans.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(SlotReaderMessageProducer::class)
    open class LegacyProducerConfiguration {
        @Bean
        @ConditionalOnBean(SqsTemplate::class)
        @ConditionalOnProperty(prefix = "cdc.outbox.dead-letter", name = ["queue-name"])
        @ConditionalOnMissingBean
        open fun cdcOutboxDeadLetterSink(
            sqsTemplate: SqsTemplate,
            properties: CdcOutboxProperties,
        ): DeadLetterSink {
            val queue =
                requireNotNull(properties.deadLetter.queueName) {
                    "cdc.outbox.dead-letter.queue-name must be set when the DLQ bean is wired"
                }
            return SqsDeadLetterSink(sqsTemplate, queue)
        }

        // LongParameterList: Spring @Bean factories collect collaborators by
        // bean type — grouping them into a wrapper object would force a
        // parallel POJO that adds zero semantics. The legacy producer
        // legitimately depends on 10 wired beans.
        @Suppress("LongParameterList")
        @Bean
        @ConditionalOnMissingBean
        open fun slotReaderMessageProducer(
            postgresConfiguration: PostgresConfiguration,
            replicationConfiguration: ReplicationConfiguration,
            snsProducer: SNSProducer,
            sqsProducer: SQSProducer,
            connectionProvider: ConnectionProvider,
            metrics: CdcOutboxMetrics,
            @Qualifier("cdcOutboxReconnectBackOff") reconnectBackOff: BackOff,
            @Qualifier("cdcOutboxPublishBackOff") publishBackOff: BackOff,
            deadLetterSink: DeadLetterSink?,
            properties: CdcOutboxProperties,
        ): SlotReaderMessageProducer =
            SlotReaderMessageProducer(
                postgresConfiguration = postgresConfiguration,
                replicationConfiguration = replicationConfiguration,
                snsProducer = snsProducer,
                sqsProducer = sqsProducer,
                connectionProvider = connectionProvider,
                metrics = metrics,
                reconnectBackOff = reconnectBackOff,
                maxReconnectAttempts = properties.retry.maxReconnectAttempts,
                deadLetterSink = deadLetterSink,
                maxPublishAttempts = properties.retry.maxPublishAttempts,
                publishBackOff = publishBackOff,
            )

        /**
         * Legacy lifecycle. Wave 5 flipped the default to `hexagonal`,
         * so this bean ONLY wires when the consumer explicitly sets
         * `cdc.outbox.processor.kind=legacy`. The new
         * [CdcProcessorLifecycle] takes over otherwise — the two
         * lifecycles are mutually exclusive to avoid spawning competing
         * streaming threads.
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(
            prefix = "cdc.outbox.processor",
            name = ["kind"],
            havingValue = "legacy",
        )
        open fun cdcOutboxLifecycle(producer: SlotReaderMessageProducer): CdcOutboxLifecycle =
            CdcOutboxLifecycle(producer)
    }
}
