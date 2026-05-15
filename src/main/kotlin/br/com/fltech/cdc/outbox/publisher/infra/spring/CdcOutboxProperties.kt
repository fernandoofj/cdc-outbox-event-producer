package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.domain.TableMapping
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

    @NestedConfigurationProperty
    val deadLetter: DeadLetter = DeadLetter(),

    @NestedConfigurationProperty
    val processor: Processor = Processor(),

    /**
     * Declarative table-mapping list (Wave 3.5 / item 7 of the brief).
     * Each entry binds one source-table FQ name to its outbound shape
     * (key, payload projection, eventType template, sink + attributes).
     * Consumed by `core.application.DefaultMappingRules`.
     *
     * Bindable via YAML:
     * ```
     * cdc.outbox.mappings:
     *   - table: public.orders
     *     capture: [INSERT, UPDATE, DELETE]
     *     key:
     *       columns: [id]
     *       format: "order:{id}"
     *     payload:
     *       include: [id, status, total_cents]
     *       rename:
     *         total_cents: totalCents
     *     event-type:
     *       template: "orders.{op}"
     *     routing:
     *       sink: sns://orders-events
     *       attributes:
     *         tenant: "{tenant_id}"
     * ```
     */
    val mappings: List<MappingProps> = emptyList(),
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
        /** Maximum publish attempts per message INCLUDING the first try. */
        val maxPublishAttempts: Int = 5,
        /** Initial delay between publish retries on the same message. */
        val publishBackoffInitial: Duration = Duration.ofMillis(100),
        /** Cap on the publish-retry delay. */
        val publishBackoffMax: Duration = Duration.ofSeconds(5),
    )

    data class DeadLetter(
        /**
         * SQS queue name (or ARN) to forward dead-lettered messages to.
         * When null or blank, the dead-letter sink is NOT wired and
         * messages that exhaust their retries leave the slot stuck —
         * operator must intervene. Set this for production.
         */
        val queueName: String? = null,
    )

    /**
     * Spring-friendly mirror of [TableMapping] for `@ConfigurationProperties`
     * binding. Translated to the immutable [TableMapping] domain type
     * by `CdcOutboxMappingAutoConfiguration` at startup.
     */
    data class MappingProps(
        val table: String = "",
        val capture: Set<RowChange.Op> = RowChange.Op.values().toSet(),
        val key: KeyProps = KeyProps(),
        val payload: PayloadProps = PayloadProps(),
        val eventType: EventTypeProps = EventTypeProps(),
        val routing: RoutingProps = RoutingProps(),
    ) {
        fun toDomain(): TableMapping = TableMapping(
            table = table,
            capture = capture,
            key = TableMapping.Key(columns = key.columns, format = key.format),
            payload = TableMapping.Payload(
                include = payload.include,
                exclude = payload.exclude,
                rename = payload.rename,
            ),
            eventType = TableMapping.EventType(template = eventType.template),
            routing = TableMapping.Routing(sink = routing.sink, attributes = routing.attributes),
        )
    }

    data class KeyProps(
        val columns: List<String> = emptyList(),
        val format: String? = null,
    )

    data class PayloadProps(
        val include: List<String> = emptyList(),
        val exclude: List<String> = emptyList(),
        val rename: Map<String, String> = emptyMap(),
    )

    data class EventTypeProps(
        val template: String = TableMapping.EventType.DEFAULT_TEMPLATE,
    )

    data class RoutingProps(
        val sink: String = "",
        val attributes: Map<String, String> = emptyMap(),
    )

    data class Processor(
        /**
         * Which orchestrator to wire.
         *  - `HEXAGONAL` (default since Wave 5): the
         *    [br.com.fltech.cdc.outbox.publisher.core.application.CdcProcessor]
         *    driven by the [br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry]
         *    — pluggable sinks, routing by scheme.
         *  - `LEGACY`: the monolithic
         *    [br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer]
         *    with its hard-coded `SNS|/SQS|` prefix switch. Kept so
         *    consumers who depended on the old wiring during Waves
         *    3 + 4 can continue to run unchanged. Set
         *    `cdc.outbox.processor.kind=legacy` to opt back in.
         */
        val kind: Kind = Kind.HEXAGONAL,
    ) {
        enum class Kind { LEGACY, HEXAGONAL }
    }

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
