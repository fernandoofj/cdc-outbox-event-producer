package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.cdc.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.cdc.outbox.publisher.core.application.CdcProcessor
import br.com.fltech.cdc.outbox.publisher.core.application.MappingCdcSource
import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource
import br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource
import br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Asserts the mutual-exclusion contract of `cdc.outbox.processor.kind`
 * across the auto-configurations that participate in it.
 *
 * The default since Wave 5 is HEXAGONAL — verify that an unset
 * property does NOT silently fall back to LEGACY (the prior default)
 * AND that the health indicator follows whichever chain wires.
 *
 * Each scenario builds its own runner with ONLY the lifecycle override
 * that the active chain needs, so `containsBean` assertions test the
 * production wiring, not the test scaffolding.
 */
class ProcessorKindToggleTest {

    private val baseProps = arrayOf(
        "cdc.outbox.postgres.host=db.example.com",
        "cdc.outbox.postgres.username=replica",
        "cdc.outbox.postgres.password=secret",
        "cdc.outbox.postgres.database=appdb",
        "cdc.outbox.replication.slot-name=test_slot",
    )

    private val autoConfigs = AutoConfigurations.of(
        CdcOutboxAutoConfiguration::class.java,
        CdcOutboxHexagonalAutoConfiguration::class.java,
        CdcOutboxSinkAutoConfiguration::class.java,
        CdcOutboxHealthAutoConfiguration::class.java,
    )

    @Test
    fun `unset processor-kind wires the HEXAGONAL chain and the hex health indicator`() {
        ApplicationContextRunner()
            .withConfiguration(autoConfigs)
            .withUserConfiguration(StubSinks::class.java, NoOpHexLifecycle::class.java)
            .withPropertyValues(*baseProps)
            .run { ctx ->
                assertNotNull(ctx.getBean(CdcProcessor::class.java))
                assertTrue(ctx.containsBean("cdcOutboxProcessorLifecycle"))
                // Hex chain wired → legacy lifecycle is NOT produced by the auto-config
                // (default processor.kind is HEXAGONAL, so the legacy bean's
                // @ConditionalOnProperty(havingValue="legacy") doesn't match).
                assertFalse(ctx.containsBean("cdcOutboxLifecycle"))
                val indicator = ctx.getBean("cdcOutboxHealthIndicator", HealthIndicator::class.java)
                assertTrue(
                    indicator is CdcProcessorHealthIndicator,
                    "expected hex health indicator, got ${indicator.javaClass.simpleName}",
                )
            }
    }

    @Test
    fun `processor-kind=hexagonal explicitly wires the hex chain and indicator`() {
        ApplicationContextRunner()
            .withConfiguration(autoConfigs)
            .withUserConfiguration(StubSinks::class.java, NoOpHexLifecycle::class.java)
            .withPropertyValues(*baseProps, "cdc.outbox.processor.kind=hexagonal")
            .run { ctx ->
                assertNotNull(ctx.getBean(CdcProcessor::class.java))
                assertTrue(ctx.containsBean("cdcOutboxProcessorLifecycle"))
                assertFalse(ctx.containsBean("cdcOutboxLifecycle"))
                val indicator = ctx.getBean("cdcOutboxHealthIndicator", HealthIndicator::class.java)
                assertTrue(indicator is CdcProcessorHealthIndicator)
            }
    }

    @Test
    fun `a consumer-supplied RowChangeSource is wrapped in MappingCdcSource by the hex auto-config`() {
        // Opt-in path added in Wave 5.2: a consumer registers a
        // RowChangeSource bean (MySQL binlog, Postgres WAL row source,
        // their own custom adapter) and the auto-config wires the
        // CdcSource via MappingCdcSource — picking up MappingRules
        // from CdcOutboxMappingAutoConfiguration on the way. The
        // legacy PgLogicalReplicationCdcSource default does NOT apply.
        ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    CdcOutboxAutoConfiguration::class.java,
                    CdcOutboxHexagonalAutoConfiguration::class.java,
                    CdcOutboxSinkAutoConfiguration::class.java,
                    CdcOutboxHealthAutoConfiguration::class.java,
                    CdcOutboxMappingAutoConfiguration::class.java,
                ),
            )
            .withUserConfiguration(
                StubSinks::class.java,
                NoOpHexLifecycleWithRowSource::class.java,
            )
            .withPropertyValues(*baseProps)
            .run { ctx ->
                val source = ctx.getBean("cdcOutboxSource", CdcSource::class.java)
                assertTrue(
                    source is MappingCdcSource,
                    "expected MappingCdcSource, got ${source.javaClass.simpleName}",
                )
            }
    }

    @Test
    fun `processor-kind=legacy wires the legacy lifecycle, the legacy indicator, and no hex beans`() {
        ApplicationContextRunner()
            .withConfiguration(autoConfigs)
            .withUserConfiguration(StubSinks::class.java, NoOpLegacyLifecycle::class.java)
            .withPropertyValues(*baseProps, "cdc.outbox.processor.kind=legacy")
            .run { ctx ->
                assertNotNull(ctx.getBean(CdcOutboxLifecycle::class.java))
                assertNotNull(ctx.getBean(SlotReaderMessageProducer::class.java))
                // The hex auto-config's @ConditionalOnProperty rejects `legacy`,
                // so none of its beans should be present.
                assertFalse(ctx.containsBean("cdcOutboxProcessor"))
                assertFalse(ctx.containsBean("cdcOutboxProcessorLifecycle"))
                assertFalse(ctx.containsBean("cdcOutboxSource"))
                val indicator = ctx.getBean("cdcOutboxHealthIndicator", HealthIndicator::class.java)
                assertTrue(
                    indicator is CdcOutboxHealthIndicator,
                    "expected legacy health indicator, got ${indicator.javaClass.simpleName}",
                )
            }
    }

    /**
     * SNS/SQS stubs — required by the legacy `SlotReaderMessageProducer`
     * bean even on the hex path (it's still in the context, just not
     * lifecycle-driven).
     */
    @Configuration
    open class StubSinks {
        @Bean
        open fun snsProducer(): SNSProducer = object : SNSProducer {
            override fun <T : Any> send(topicName: String, message: SNSMessage<T>) = Unit
        }

        @Bean
        open fun sqsProducer(): SQSProducer = object : SQSProducer {
            override fun <T : Any> send(queueName: String, message: T) = Unit
        }
    }

    /**
     * Hex-scenario override: stub the [CdcSource] (so the auto-config's
     * default `PgLogicalReplicationCdcSource` doesn't try to reach a
     * real Postgres) and the [CdcProcessorLifecycle] (so the context
     * refresh doesn't spawn a real streaming thread).
     */
    @Configuration
    open class NoOpHexLifecycle {
        @Bean
        open fun cdcOutboxSource(): CdcSource = mockk(relaxed = true)

        @Bean
        open fun cdcOutboxProcessorLifecycle(): CdcProcessorLifecycle =
            CdcProcessorLifecycle(mockk(relaxed = true))
    }

    /**
     * Hex + row-source scenario: stub a [RowChangeSource] bean so the
     * auto-config takes the Wave 5.2 `MappingCdcSource` branch
     * instead of falling through to the default
     * `PgLogicalReplicationCdcSource`.
     */
    @Configuration
    open class NoOpHexLifecycleWithRowSource {
        @Bean
        open fun cdcOutboxRowSource(): RowChangeSource = mockk(relaxed = true)

        @Bean
        open fun cdcOutboxProcessorLifecycle(): CdcProcessorLifecycle =
            CdcProcessorLifecycle(mockk(relaxed = true))
    }

    /**
     * Legacy-scenario override: stub the [CdcOutboxLifecycle] so the
     * context refresh doesn't spin up the real
     * `SlotReaderMessageProducer` streaming thread.
     */
    @Configuration
    open class NoOpLegacyLifecycle {
        @Bean
        open fun cdcOutboxLifecycle(): CdcOutboxLifecycle =
            CdcOutboxLifecycle(mockk(relaxed = true))
    }
}
