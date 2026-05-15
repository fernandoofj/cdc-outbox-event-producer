package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.cdc.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.cdc.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.cdc.outbox.publisher.deadletter.SqsDeadLetterSink
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.cdc.outbox.publisher.replication.connector.HikariCPConnectionProvider
import br.com.fltech.cdc.outbox.publisher.retry.BackOff
import br.com.fltech.cdc.outbox.publisher.retry.ExponentialBackOff
import br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer
import io.awspring.cloud.sqs.operations.SqsTemplate
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

/**
 * Wires the CDC-outbox producer into a Spring Boot 3 application.
 *
 * The whole configuration is gated by `cdc.outbox.enabled` (default `true`)
 * and by the presence of the producer class on the classpath. Every bean
 * uses `@ConditionalOnMissingBean` so a consumer can override any single
 * piece (e.g. inject a custom [ConnectionProvider] for tests) without
 * disabling the rest of the chain.
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
 *  - [SlotReaderMessageProducer]: composed from the beans above plus
 *    user-provided [SNSProducer] and [SQSProducer] beans.
 *  - [CdcOutboxLifecycle]: starts/stops the producer with the Spring
 *    lifecycle.
 *  - [CdcOutboxHealthIndicator]: registered only when the Actuator
 *    HealthIndicator class is on the classpath.
 */
@AutoConfiguration
@ConditionalOnClass(SlotReaderMessageProducer::class)
@ConditionalOnProperty(prefix = "cdc.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CdcOutboxProperties::class)
open class CdcOutboxAutoConfiguration {

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
            ),
        )

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
    open fun cdcOutboxMetrics(meterRegistry: MeterRegistry?): CdcOutboxMetrics =
        CdcOutboxMetrics(meterRegistry)

    @Bean("cdcOutboxPublishBackOff")
    @ConditionalOnMissingBean(name = ["cdcOutboxPublishBackOff"])
    open fun cdcOutboxPublishBackOff(properties: CdcOutboxProperties): BackOff =
        ExponentialBackOff(
            initial = properties.retry.publishBackoffInitial,
            max = properties.retry.publishBackoffMax,
            multiplier = properties.retry.multiplier,
            jitter = properties.retry.jitter,
        )

    @Bean
    @ConditionalOnBean(SqsTemplate::class)
    @ConditionalOnProperty(prefix = "cdc.outbox.dead-letter", name = ["queue-name"])
    @ConditionalOnMissingBean
    open fun cdcOutboxDeadLetterSink(
        sqsTemplate: SqsTemplate,
        properties: CdcOutboxProperties,
    ): DeadLetterSink {
        val queue = requireNotNull(properties.deadLetter.queueName) {
            "cdc.outbox.dead-letter.queue-name must be set when the DLQ bean is wired"
        }
        return SqsDeadLetterSink(sqsTemplate, queue)
    }

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

    @Bean
    @ConditionalOnMissingBean
    open fun cdcOutboxLifecycle(producer: SlotReaderMessageProducer): CdcOutboxLifecycle =
        CdcOutboxLifecycle(producer)

    // CdcOutboxHealthIndicator is contributed by CdcOutboxHealthAutoConfiguration
    // — split into its own file because the class-level `import` of
    // org.springframework.boot.actuate.health.HealthIndicator would otherwise
    // fail to resolve when consumers run without spring-boot-actuator on the
    // classpath. `@ConditionalOnClass` only gates per-bean evaluation, not
    // class-load resolution of the enclosing @Configuration class itself.
}
