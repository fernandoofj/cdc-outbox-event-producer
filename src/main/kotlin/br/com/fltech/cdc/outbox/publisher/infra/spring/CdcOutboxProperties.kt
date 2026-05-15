package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.replication.enums.FormatVersionEnum
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.NestedConfigurationProperty
import java.time.Duration

/**
 * Externalised configuration for the CDC-outbox producer.
 *
 * The whole surface lives under the `cdc.outbox.*` prefix; sections mirror
 * the runtime collaborators they configure (source, replication slot, pool,
 * retry, observability). Defaults match the constants in
 * [br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration],
 * [br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration],
 * [br.com.fltech.cdc.outbox.publisher.replication.connector.HikariCPConnectionProvider.PoolConfig]
 * and [br.com.fltech.cdc.outbox.publisher.retry.ExponentialBackOff].
 *
 * Set `cdc.outbox.enabled = false` (or simply omit the configuration) to
 * keep the auto-configuration dormant.
 */
@ConfigurationProperties(prefix = "cdc.outbox")
data class CdcOutboxProperties(
    /** Master switch. When false the auto-configuration registers no beans. */
    val enabled: Boolean = true,

    @NestedConfigurationProperty
    val postgres: Postgres = Postgres(),

    @NestedConfigurationProperty
    val replication: Replication = Replication(),

    @NestedConfigurationProperty
    val pool: Pool = Pool(),

    @NestedConfigurationProperty
    val retry: Retry = Retry(),

    @NestedConfigurationProperty
    val health: Health = Health(),
) {

    data class Postgres(
        val host: String = "localhost",
        val port: String = "5432",
        val database: String = "postgres",
        val username: String = "postgres",
        val password: String = "",
        val sslMode: String = "disable",
        val pathToRootCert: String? = null,
        val sslPassword: String? = null,
        val pathToSslKey: String? = null,
        val pathToSslCert: String? = null,
    )

    data class Replication(
        /** Required when [enabled] = true. Lower-snake-case, ≤ 63 chars. */
        val slotName: String = "cdc_outbox_slot",
        val outputPlugin: String = "wal2json",
        val statusInterval: Duration = Duration.ofSeconds(20),
        val updateIdleSlotInterval: Duration = Duration.ofMinutes(5),
        val existingProcessRetryLimit: Int? = 30,
        val existingProcessRetrySleep: Duration? = Duration.ofSeconds(30),
        val includeXids: Boolean = true,
        val includeLsn: Boolean = true,
        val formatVersion: FormatVersionEnum = FormatVersionEnum.V2,
    )

    data class Pool(
        val maximumPoolSize: Int = 2,
        val minimumIdle: Int = 0,
        val connectionTimeout: Duration = Duration.ofSeconds(5),
        val validationTimeout: Duration = Duration.ofSeconds(3),
        val idleTimeout: Duration = Duration.ofMinutes(5),
        val maxLifetime: Duration = Duration.ofMinutes(30),
        val leakDetectionThreshold: Duration = Duration.ofSeconds(30),
        val poolName: String = "cdc-outbox-query-pool",
    )

    data class Retry(
        val initial: Duration = Duration.ofMillis(200),
        val max: Duration = Duration.ofSeconds(30),
        val multiplier: Double = 2.0,
        val jitter: Double = 0.3,
        val maxReconnectAttempts: Int = 30,
    )

    data class Health(
        /**
         * If the slot has been idle (no readPending hits) for longer than
         * this, the [CdcOutboxHealthIndicator] reports `OUT_OF_SERVICE`
         * rather than `UP`. A pending publish failure always degrades the
         * indicator regardless of this knob.
         */
        val maxIdle: Duration = Duration.ofMinutes(10),
    )
}
