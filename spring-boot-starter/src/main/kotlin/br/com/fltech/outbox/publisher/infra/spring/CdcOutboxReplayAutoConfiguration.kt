package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.adapter.replay.PgWalReplayerStub
import br.com.fltech.outbox.publisher.adapter.replay.ReplayActuatorEndpoint
import br.com.fltech.outbox.publisher.adapter.replay.ReplayService
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.core.port.MappingRules
import br.com.fltech.outbox.publisher.core.port.SourceReplayer
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean

/**
 * Wires the source-side replay tooling.
 *
 * Defence-in-depth on authentication mirrors the DLQ replay
 * auto-config: refuse-to-wire if Spring Security is absent, plus
 * a runtime `requireAuthenticated()` check inside every endpoint
 * method.
 *
 * Wired only when `cdc.outbox.replay.enabled=true`. Postgres
 * source kind is intentionally registered as the
 * `PgWalReplayerStub` so a Postgres-targeted replay fails fast
 * with an explicit message instead of silently emitting nothing.
 */
@AutoConfiguration
@AutoConfigureAfter(
    CdcOutboxHexagonalAutoConfiguration::class,
    CdcOutboxMappingAutoConfiguration::class,
)
@ConditionalOnProperty(
    prefix = "cdc.outbox.replay",
    name = ["enabled"],
    havingValue = "true",
)
@ConditionalOnClass(
    name = [
        "org.springframework.security.web.SecurityFilterChain",
        "org.springframework.boot.actuate.endpoint.annotation.Endpoint",
    ],
)
@ConditionalOnBean(value = [EventSinkRegistry::class, MappingRules::class])
open class CdcOutboxReplayAutoConfiguration {

    /**
     * Postgres replayer is registered as the explicit stub. A
     * future round can replace this bean with a real implementation
     * — the standard `@ConditionalOnMissingBean(name = ...)`
     * rule applies.
     *
     * MySQL replayer is intentionally NOT auto-wired here: the
     * consumer has to register a `MySqlBinlogReplayer` bean
     * themselves (it needs host/port/user/password — credentials
     * that are NOT part of `CdcOutboxProperties` and that the
     * consumer already provides for the live binlog source).
     * The replay service picks up every `SourceReplayer` bean
     * present in the context via constructor injection of a list.
     */
    @Bean("cdcOutboxPostgresReplayer")
    @ConditionalOnMissingBean(name = ["cdcOutboxPostgresReplayer"])
    open fun cdcOutboxPostgresReplayer(): SourceReplayer = PgWalReplayerStub()

    @Bean
    @ConditionalOnMissingBean(ReplayService::class)
    open fun cdcOutboxReplayService(
        replayers: List<SourceReplayer>,
        mappingRules: MappingRules,
        sinkRegistry: EventSinkRegistry,
        metrics: CdcOutboxMetrics,
        properties: CdcOutboxProperties,
    ): ReplayService = ReplayService(
        replayers = replayers.associateBy { it.sourceKind },
        mappingRules = mappingRules,
        sinkRegistry = sinkRegistry,
        metrics = metrics,
        maxEventsPerJob = properties.replay.maxEventsPerJob,
        jobTimeoutMs = properties.replay.timeoutMs,
    )

    @Bean
    @ConditionalOnMissingBean(ReplayActuatorEndpoint::class)
    open fun cdcOutboxReplayEndpoint(service: ReplayService): ReplayActuatorEndpoint =
        ReplayActuatorEndpoint(service)
}
