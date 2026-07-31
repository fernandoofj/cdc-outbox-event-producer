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
 */
@AutoConfiguration
@AutoConfigureAfter(CdcOutboxAutoConfiguration::class)
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
