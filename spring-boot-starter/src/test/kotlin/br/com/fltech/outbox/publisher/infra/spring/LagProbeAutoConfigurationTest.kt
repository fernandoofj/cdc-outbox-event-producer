package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.adapter.lag.mysql.MysqlLagProbe
import br.com.fltech.outbox.publisher.adapter.lag.postgres.PostgresLagProbe
import br.com.fltech.outbox.publisher.adapter.source.mysql.MySqlBinlogRowChangeSource
import br.com.fltech.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource
import br.com.fltech.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.outbox.publisher.core.port.CheckpointStore
import br.com.fltech.outbox.publisher.core.port.InMemoryCheckpointStore
import br.com.fltech.outbox.publisher.core.port.LagProbe
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Asserts the new lag-probe wiring picks the right adapter based on
 * which `RowChangeSource` the consumer registered, and that the
 * `cdc.outbox.lag.enabled=false` switch genuinely turns the whole
 * subsystem off.
 *
 * Lifecycle is stubbed so we don't spawn the orchestrator or the
 * scheduler in the test context.
 */
class LagProbeAutoConfigurationTest {
    private val baseProps =
        arrayOf(
            "cdc.outbox.postgres.host=db.example.com",
            "cdc.outbox.postgres.username=replica",
            "cdc.outbox.postgres.password=secret",
            "cdc.outbox.postgres.database=appdb",
            "cdc.outbox.replication.slot-name=lag_probe_slot",
        )

    private val autoConfigs =
        AutoConfigurations.of(
            CdcOutboxAutoConfiguration::class.java,
            CdcOutboxHexagonalAutoConfiguration::class.java,
            CdcOutboxSinkAutoConfiguration::class.java,
        )

    @Test
    fun `Postgres row source pulls in a PostgresLagProbe and the scheduler`() {
        ApplicationContextRunner()
            .withConfiguration(autoConfigs)
            .withUserConfiguration(
                StubSinks::class.java,
                NoOpHexLifecycle::class.java,
                WithPgRowSource::class.java,
            )
            .withPropertyValues(*baseProps)
            .run { ctx ->
                val probe = ctx.getBean(LagProbe::class.java)
                assertTrue(probe is PostgresLagProbe, "expected PostgresLagProbe got ${probe.javaClass.simpleName}")
                assertTrue(ctx.containsBean("cdcOutboxLagProbeScheduler"))
            }
    }

    @Test
    fun `MySQL binlog source pulls in a MysqlLagProbe when DataSource and CheckpointStore are present`() {
        ApplicationContextRunner()
            .withConfiguration(autoConfigs)
            .withUserConfiguration(
                StubSinks::class.java,
                NoOpHexLifecycle::class.java,
                WithMysqlBinlogSource::class.java,
            )
            .withPropertyValues(*baseProps)
            .run { ctx ->
                val probe = ctx.getBean(LagProbe::class.java)
                assertTrue(probe is MysqlLagProbe, "expected MysqlLagProbe got ${probe.javaClass.simpleName}")
                assertTrue(ctx.containsBean("cdcOutboxLagProbeScheduler"))
            }
    }

    @Test
    fun `cdc-outbox-lag-enabled=false disables probe, scheduler, and the gauge`() {
        ApplicationContextRunner()
            .withConfiguration(autoConfigs)
            .withUserConfiguration(
                StubSinks::class.java,
                NoOpHexLifecycle::class.java,
                WithPgRowSource::class.java,
            )
            .withPropertyValues(*baseProps, "cdc.outbox.lag.enabled=false")
            .run { ctx ->
                assertTrue(ctx.getBeansOfType(LagProbe::class.java).isEmpty())
                assertFalse(ctx.containsBean("cdcOutboxLagProbeScheduler"))
            }
    }

    @Test
    fun `no row source means no LagProbe`() {
        ApplicationContextRunner()
            .withConfiguration(autoConfigs)
            .withUserConfiguration(StubSinks::class.java, NoOpHexLifecycle::class.java)
            .withPropertyValues(*baseProps)
            .run { ctx ->
                assertTrue(ctx.getBeansOfType(LagProbe::class.java).isEmpty())
                assertFalse(ctx.containsBean("cdcOutboxLagProbeScheduler"))
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
    open class WithPgRowSource {
        // We register the *concrete* PgWalRowChangeSource (not just a
        // RowChangeSource) so the auto-config's
        // @ConditionalOnBean(PgWalRowChangeSource::class) matches.
        // Using mockk(relaxed = true) keeps construction cheap and
        // avoids touching real replication wiring.
        @Bean
        open fun pgWalRowChangeSource(): PgWalRowChangeSource = mockk(relaxed = true)
    }

    @Configuration
    open class WithMysqlBinlogSource {
        @Bean
        open fun mysqlBinlogRowChangeSource(): MySqlBinlogRowChangeSource = mockk(relaxed = true)

        @Bean
        open fun dataSource(): DataSource = mockk(relaxed = true)

        @Bean
        open fun checkpointStore(): CheckpointStore = InMemoryCheckpointStore()
    }
}
