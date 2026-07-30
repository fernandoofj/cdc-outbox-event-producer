package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.core.application.CdcProcessor
import br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Wires the Actuator [HealthIndicator] for whichever orchestrator the
 * consumer ended up running.
 *
 *  - When the LEGACY chain is active (`SlotReaderMessageProducer` +
 *    `CdcOutboxLifecycle` present) → [CdcOutboxHealthIndicator]:
 *    DOWN on pending failure, OUT_OF_SERVICE on idle, UP otherwise.
 *  - When the HEXAGONAL chain is active (`CdcProcessor` +
 *    `CdcProcessorLifecycle` present) → [CdcProcessorHealthIndicator]:
 *    DOWN when the lifecycle is up but the processor loop is not
 *    iterating, UP otherwise. Pending-failure / idle reporting on the
 *    hex path lands in Wave 5.1.
 *
 * The two branches are nested `@Configuration` classes so each is
 * evaluated independently — having two top-level `@Bean` methods
 * declare the same bean name within a single `@Configuration` confuses
 * Spring's `@ConditionalOnMissingBean` resolution and skips both. The
 * lifecycles never coexist (gated by `cdc.outbox.processor.kind`), so
 * exactly one nested config activates per context.
 *
 * The file lives outside [CdcOutboxAutoConfiguration] because class-
 * level `import`s of `HealthIndicator` would crash class loading on
 * consumers that pull the starter without `spring-boot-health` (Boot 4
 * split Health out of `spring-boot-actuator` into its own module).
 */
@AutoConfiguration
// Must run after BOTH the legacy and hexagonal configs so the
// nested `@ConditionalOnBean(CdcProcessor::class, ...)` evaluation
// can actually see those beans when they exist.
@AutoConfigureAfter(CdcOutboxAutoConfiguration::class, CdcOutboxHexagonalAutoConfiguration::class)
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnProperty(prefix = "cdc.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CdcOutboxProperties::class)
open class CdcOutboxHealthAutoConfiguration {

    /** Legacy chain (`processor.kind=legacy`). */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(value = [SlotReaderMessageProducer::class, CdcOutboxLifecycle::class])
    open class LegacyHealth {
        @Bean("cdcOutboxHealthIndicator")
        @ConditionalOnMissingBean(name = ["cdcOutboxHealthIndicator"])
        open fun cdcOutboxHealthIndicator(
            producer: SlotReaderMessageProducer,
            lifecycle: CdcOutboxLifecycle,
            properties: CdcOutboxProperties,
        ): CdcOutboxHealthIndicator =
            CdcOutboxHealthIndicator(producer, lifecycle, properties.health.maxIdle)
    }

    /** Hexagonal chain (`processor.kind=hexagonal`, the Wave 5 default). */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(value = [CdcProcessor::class, CdcProcessorLifecycle::class])
    open class HexagonalHealth {
        @Bean("cdcOutboxHealthIndicator")
        @ConditionalOnMissingBean(name = ["cdcOutboxHealthIndicator"])
        open fun cdcOutboxHealthIndicator(
            processor: CdcProcessor,
            lifecycle: CdcProcessorLifecycle,
            properties: CdcOutboxProperties,
        ): CdcProcessorHealthIndicator =
            CdcProcessorHealthIndicator(processor, lifecycle, properties.health.maxIdle)
    }
}
