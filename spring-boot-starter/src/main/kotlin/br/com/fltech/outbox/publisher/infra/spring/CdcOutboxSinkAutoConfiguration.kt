package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.adapter.sink.kafka.KafkaEventSink
import br.com.fltech.outbox.publisher.adapter.sink.rabbitmq.RabbitMqEventSink
import br.com.fltech.outbox.publisher.adapter.sink.registry.DefaultEventSinkRegistry
import br.com.fltech.outbox.publisher.adapter.sink.sns.SnsEventSink
import br.com.fltech.outbox.publisher.adapter.sink.sqs.SqsEventSink
import br.com.fltech.outbox.publisher.core.port.EventSink
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import io.awspring.cloud.sns.core.SnsTemplate
import io.awspring.cloud.sqs.operations.SqsTemplate
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate

/**
 * Registers an [EventSink] per broker scheme — `sns`, `sqs`, `kafka`,
 * `amqp` — gated by the presence of the corresponding template bean on
 * the application context. Builds the [EventSinkRegistry] from whatever
 * sinks ended up registered.
 *
 * Each sink class is in its own nested `@Configuration` so the
 * top-level autoconfig can be loaded even when `spring-kafka` or
 * `spring-rabbit` are absent from the consumer classpath
 * (class-level imports inside this file are intentionally limited to
 * the classes used by the outer trigger; nested configs carry the
 * broker-specific imports).
 *
 * Activated only when `cdc.outbox.enabled=true` (default).
 *
 * `@AutoConfigureAfter(name = [...])`, not the class-literal form:
 * without an explicit ordering hint each nested sink config's
 * `@ConditionalOnBean(...Template::class)` was evaluated *before* the
 * broker's own auto-configuration (e.g. Spring Cloud AWS's
 * `SnsAutoConfiguration`) had registered that template bean's
 * definition — `@ConditionalOnBean` only sees what's been registered
 * by auto-configs processed so far, not the final application state,
 * so every sink silently failed to wire even with the right
 * dependency on the classpath (`EventSinkRegistry` ended up with zero
 * known schemes). The string form avoids a compile-time dependency on
 * these optional broker autoconfiguration classes, mirroring why the
 * sink adapters themselves are `compileOnly`.
 */
@AutoConfiguration
@AutoConfigureAfter(
    value = [CdcOutboxAutoConfiguration::class],
    name = [
        "io.awspring.cloud.autoconfigure.sns.SnsAutoConfiguration",
        "io.awspring.cloud.autoconfigure.sqs.SqsAutoConfiguration",
        // Boot 4 exploded per-technology autoconfigs out of
        // spring-boot-autoconfigure into their own modules under a new
        // package (org.springframework.boot.<tech>.autoconfigure.*) —
        // NOT org.springframework.boot.autoconfigure.<tech>.* (the Boot
        // 3 location). AutoConfigurationSorter silently ignores names it
        // can't resolve, so a stale Boot-3-era FQN here would make this
        // ordering fix a silent no-op instead of a compile error.
        "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
        "org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration",
    ],
)
@ConditionalOnProperty(prefix = "cdc.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
open class CdcOutboxSinkAutoConfiguration {
    @Bean("cdcOutboxSinkRegistry")
    @ConditionalOnMissingBean(name = ["cdcOutboxSinkRegistry"])
    open fun cdcOutboxSinkRegistry(eventSinks: Map<String, EventSink>): EventSinkRegistry {
        // The bean name → EventSink map is keyed by the bean name we
        // give each adapter below ("sns", "sqs", "kafka", "amqp"). That
        // becomes the routing scheme.
        return DefaultEventSinkRegistry(eventSinks)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(SnsTemplate::class)
    open class SnsSinkConfig {
        @Bean(name = ["sns"])
        @ConditionalOnBean(SnsTemplate::class)
        @ConditionalOnMissingBean(name = ["sns"])
        open fun snsEventSink(snsTemplate: SnsTemplate): EventSink = SnsEventSink(snsTemplate)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(SqsTemplate::class)
    open class SqsSinkConfig {
        @Bean(name = ["sqs"])
        @ConditionalOnBean(SqsTemplate::class)
        @ConditionalOnMissingBean(name = ["sqs"])
        open fun sqsEventSink(sqsTemplate: SqsTemplate): EventSink = SqsEventSink(sqsTemplate)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KafkaTemplate::class)
    open class KafkaSinkConfig {
        @Bean(name = ["kafka"])
        @ConditionalOnBean(KafkaTemplate::class)
        @ConditionalOnMissingBean(name = ["kafka"])
        @Suppress("UNCHECKED_CAST")
        open fun kafkaEventSink(kafkaTemplate: KafkaTemplate<*, *>): EventSink =
            KafkaEventSink(kafkaTemplate as KafkaTemplate<String, ByteArray>)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(RabbitTemplate::class)
    open class RabbitSinkConfig {
        @Bean(name = ["amqp"])
        @ConditionalOnBean(RabbitTemplate::class)
        @ConditionalOnMissingBean(name = ["amqp"])
        open fun rabbitMqEventSink(rabbitTemplate: RabbitTemplate): EventSink = RabbitMqEventSink(rabbitTemplate)
    }
}
