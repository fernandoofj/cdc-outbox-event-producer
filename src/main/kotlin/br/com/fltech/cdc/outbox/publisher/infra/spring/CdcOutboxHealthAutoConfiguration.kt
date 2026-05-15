package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

/**
 * Separate auto-configuration for the Actuator health indicator. Lives in
 * its own file so that loading [CdcOutboxAutoConfiguration] does not
 * fail when `spring-boot-actuator` is absent from the consumer classpath
 * — class-level `import`s are resolved at class-load time, before
 * `@ConditionalOnClass` is consulted, so the import of [HealthIndicator]
 * MUST live in a class that is itself gated by `@ConditionalOnClass`.
 */
@AutoConfiguration
@AutoConfigureAfter(CdcOutboxAutoConfiguration::class)
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnProperty(prefix = "cdc.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CdcOutboxProperties::class)
open class CdcOutboxHealthAutoConfiguration {

    @Bean("cdcOutboxHealthIndicator")
    @ConditionalOnBean(SlotReaderMessageProducer::class, CdcOutboxLifecycle::class)
    @ConditionalOnMissingBean(name = ["cdcOutboxHealthIndicator"])
    open fun cdcOutboxHealthIndicator(
        producer: SlotReaderMessageProducer,
        lifecycle: CdcOutboxLifecycle,
        properties: CdcOutboxProperties,
    ): CdcOutboxHealthIndicator =
        CdcOutboxHealthIndicator(producer, lifecycle, properties.health.maxIdle)
}
