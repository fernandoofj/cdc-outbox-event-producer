package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.adapter.source.mysql.MySqlBinlogRowChangeSource
import br.com.fltech.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource
import br.com.fltech.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.outbox.publisher.core.application.MappingCdcSource
import br.com.fltech.outbox.publisher.core.port.CdcSource
import br.com.fltech.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.outbox.publisher.observability.LagProbeScheduler
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import kotlin.test.assertTrue

/**
 * Wiring-correctness coverage for the module-isolation fixes in
 * [CdcOutboxAutoConfiguration] and [CdcOutboxHexagonalAutoConfiguration]:
 * given a module is genuinely missing (simulated here by hiding its
 * marker class from [FilteredClassLoader], which is what
 * `@ConditionalOnBean`/`@ConditionalOnMissingBean`/`@ConditionalOnClass`
 * consult), does the CONTEXT still start and pick the right bean?
 *
 * This class does **not** cover the raw `Class.getDeclaredMethods()`
 * crash the isolation fixes exist to prevent — [FilteredClassLoader]
 * only intercepts explicit `Class.forName` lookups (what
 * `@ConditionalOnClass` uses), not the JVM's eager resolution of an
 * already-loaded class's own declared method signatures, and the
 * declaring class here is always loaded by the real, unfiltered test
 * classloader first. Verified empirically: reverting the nesting
 * fixes back to their pre-fix shape and rerunning this class stays
 * green. [AutoConfigurationClassLoadingTest] covers that mechanism
 * directly, via an isolated [java.net.URLClassLoader] over a trimmed
 * classpath — the two test classes are complementary, not redundant.
 */
class OptionalModuleAbsentTest {
    private val baseProps =
        arrayOf(
            "cdc.outbox.postgres.host=db.example.com",
            "cdc.outbox.postgres.username=replica",
            "cdc.outbox.postgres.password=secret",
            "cdc.outbox.postgres.database=appdb",
            "cdc.outbox.replication.slot-name=optional_module_absent_slot",
        )

    private val autoConfigs =
        AutoConfigurations.of(
            CdcOutboxAutoConfiguration::class.java,
            CdcOutboxHexagonalAutoConfiguration::class.java,
            CdcOutboxSinkAutoConfiguration::class.java,
        )

    @Test
    fun `hexagonal default path starts without the legacy module on the classpath`() {
        ApplicationContextRunner()
            .withClassLoader(FilteredClassLoader(SlotReaderMessageProducer::class.java, DeadLetterSink::class.java))
            .withConfiguration(autoConfigs)
            .withUserConfiguration(StubSinks::class.java, NoOpHexLifecycle::class.java)
            .withPropertyValues(*baseProps)
            .run { ctx ->
                assertTrue(ctx.startupFailure == null, "context failed to start: ${ctx.startupFailure}")
                assertTrue(ctx.containsBean("cdcOutboxSource"))
                assertTrue(ctx.getBean(CdcSource::class.java) !is MappingCdcSource, "expected the Postgres fallback")
                assertTrue(ctx.getBeansOfType(SlotReaderMessageProducer::class.java).isEmpty())
            }
    }

    @Test
    fun `lag-probe subsystem absence does not take down the rest of the hexagonal auto-config`() {
        ApplicationContextRunner()
            .withClassLoader(FilteredClassLoader(LagProbeScheduler::class.java))
            .withConfiguration(autoConfigs)
            .withUserConfiguration(StubSinks::class.java, NoOpHexLifecycle::class.java, WithPgWalRowSource::class.java)
            .withPropertyValues(*baseProps)
            .run { ctx ->
                assertTrue(ctx.startupFailure == null, "context failed to start: ${ctx.startupFailure}")
                assertTrue(ctx.getBean(CdcSource::class.java) is MappingCdcSource)
                assertTrue(ctx.getBeansOfType(LagProbeScheduler::class.java).isEmpty())
            }
    }

    @Test
    fun `MySQL lag probe absence does not take down the lag-probe subsystem`() {
        ApplicationContextRunner()
            .withClassLoader(FilteredClassLoader(MySqlBinlogRowChangeSource::class.java))
            .withConfiguration(autoConfigs)
            .withUserConfiguration(StubSinks::class.java, NoOpHexLifecycle::class.java, WithPgWalRowSource::class.java)
            .withPropertyValues(*baseProps)
            .run { ctx -> assertTrue(ctx.startupFailure == null, "context failed to start: ${ctx.startupFailure}") }
    }

    @Test
    fun `MySQL-only setup (no source-postgres, no legacy) starts and wraps the consumer's RowChangeSource`() {
        // Mirrors README's "Setup MySQL binlog + Kafka" combo, which
        // declares neither :source-postgres nor :legacy — filtering
        // only PostgresConfiguration while leaving SlotReaderMessageProducer
        // resolvable would be an unrealistic combination, since :legacy
        // itself depends on :source-postgres (api(project(":source-postgres"))),
        // so a real build can never have one absent without the other.
        ApplicationContextRunner()
            .withClassLoader(
                FilteredClassLoader(
                    PostgresConfiguration::class.java,
                    SlotReaderMessageProducer::class.java,
                    DeadLetterSink::class.java,
                ),
            )
            .withConfiguration(autoConfigs)
            .withUserConfiguration(
                StubSinks::class.java,
                NoOpHexLifecycle::class.java,
                WithMySqlBinlogRowSource::class.java,
            )
            .withPropertyValues(*baseProps)
            .run { ctx ->
                assertTrue(ctx.startupFailure == null, "context failed to start: ${ctx.startupFailure}")
                assertTrue(ctx.getBean(CdcSource::class.java) is MappingCdcSource)
            }
    }

    @Configuration
    open class StubSinks {
        @Bean
        open fun snsProducer(): SNSProducer =
            object : SNSProducer {
                override fun <T : Any> send(
                    topicName: String,
                    message: SNSMessage<T>,
                ) = Unit
            }

        @Bean
        open fun sqsProducer(): SQSProducer =
            object : SQSProducer {
                override fun <T : Any> send(
                    queueName: String,
                    message: T,
                ) = Unit
            }
    }

    @Configuration
    open class NoOpHexLifecycle {
        // Stubbed so the autoconfigured CdcProcessorLifecycle (which
        // would spawn a streaming thread) is replaced. The constructor
        // takes a mocked CdcProcessor — calling start() on it is a
        // no-op via mockk's relaxed defaults.
        @Bean
        open fun cdcOutboxProcessorLifecycle(): CdcProcessorLifecycle = CdcProcessorLifecycle(mockk(relaxed = true))
    }

    @Configuration
    open class WithPgWalRowSource {
        @Bean
        open fun pgWalRowChangeSource(): PgWalRowChangeSource = mockk(relaxed = true)
    }

    @Configuration
    open class WithMySqlBinlogRowSource {
        @Bean
        open fun mySqlBinlogRowChangeSource(): MySqlBinlogRowChangeSource = mockk(relaxed = true)
    }
}
