package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.outbox.publisher.replication.connector.HikariCPConnectionProvider
import br.com.fltech.outbox.publisher.retry.BackOff
import br.com.fltech.outbox.publisher.retry.ExponentialBackOff
import br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.sql.Connection
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Wires the auto-configuration in an isolated [ApplicationContextRunner] with
 * stubbed sinks and lifecycle so we can assert bean presence and property
 * binding without starting an actual Postgres replication loop.
 */
class CdcOutboxAutoConfigurationTest {
    private val runner =
        ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    CdcOutboxAutoConfiguration::class.java,
                    CdcOutboxHealthAutoConfiguration::class.java,
                ),
            )
            .withUserConfiguration(StubSinks::class.java, NoOpLifecycle::class.java)
            .withPropertyValues(
                "cdc.outbox.postgres.host=db.example.com",
                "cdc.outbox.postgres.username=replica",
                "cdc.outbox.postgres.password=secret",
                "cdc.outbox.postgres.database=appdb",
                "cdc.outbox.replication.slot-name=orders_outbox_slot",
                "cdc.outbox.retry.initial=500ms",
                "cdc.outbox.retry.max-reconnect-attempts=42",
            )

    @Test
    fun `binds all property groups under the cdc-outbox prefix`() {
        runner.run { ctx ->
            val props = ctx.getBean(CdcOutboxProperties::class.java)
            assertEquals("db.example.com", props.postgres.host)
            assertEquals("replica", props.postgres.username)
            assertEquals("appdb", props.postgres.database)
            assertEquals("orders_outbox_slot", props.replication.slotName)
            assertEquals(true, props.replication.includeLsn)
            assertEquals(500L, props.retry.initial.toMillis())
            assertEquals(42, props.retry.maxReconnectAttempts)
            // Default disabled (0) — CdcOutboxProperties.Pool's own default,
            // independent of HikariCPConnectionProvider's PoolConfig default.
            // See `pool keepalive-time property reaches the connection
            // provider's PoolConfig` below for the wiring between the two.
            assertEquals(0L, props.pool.keepaliveTime.toMillis())
        }
    }

    @Test
    fun `pool keepalive-time property reaches the connection provider's PoolConfig`() {
        // Reads the connection provider's private `poolConfig` via reflection
        // (see readPoolConfig below) because HikariCPConnectionProvider builds
        // pools lazily inside getConnection(), not the constructor, so the
        // bean itself never needs a live database here. This is the actual
        // regression this property exists to catch: deleting `keepaliveTime =
        // properties.pool.keepaliveTime` from CdcOutboxAutoConfiguration
        // would leave PoolConfig on its own Duration.ZERO default regardless
        // of what `cdc.outbox.pool.keepalive-time` is set to, and only an
        // assertion against PoolConfig itself — not CdcOutboxProperties —
        // would notice.
        runner.withPropertyValues("cdc.outbox.pool.keepalive-time=90s").run { ctx ->
            val poolConfig = readPoolConfig(ctx.getBean(HikariCPConnectionProvider::class.java))
            assertEquals(90_000L, poolConfig.keepaliveTime.toMillis())
        }
    }

    @Test
    fun `pool auto-commit property reaches the connection provider's PoolConfig`() {
        runner.withPropertyValues("cdc.outbox.pool.auto-commit=false").run { ctx ->
            val poolConfig = readPoolConfig(ctx.getBean(HikariCPConnectionProvider::class.java))
            assertEquals(false, poolConfig.autoCommit)
        }
    }

    private fun readPoolConfig(provider: HikariCPConnectionProvider): HikariCPConnectionProvider.PoolConfig {
        val poolConfigField = HikariCPConnectionProvider::class.java.getDeclaredField("poolConfig")
        poolConfigField.isAccessible = true
        return poolConfigField.get(provider) as HikariCPConnectionProvider.PoolConfig
    }

    @Test
    fun `derives PostgresConfiguration and ReplicationConfiguration from properties`() {
        runner.run { ctx ->
            val pg = ctx.getBean(PostgresConfiguration::class.java)
            assertEquals("db.example.com", pg.host)
            assertEquals("replica", pg.username)

            val rep = ctx.getBean(ReplicationConfiguration::class.java)
            assertEquals("orders_outbox_slot", rep.slotName)
            assertEquals(true, rep.includeLsn)
        }
    }

    @Test
    fun `registers HikariCP, BackOff, metrics, producer, lifecycle, and health indicator`() {
        runner.run { ctx ->
            assertNotNull(ctx.getBean(HikariCPConnectionProvider::class.java))
            // Two BackOff beans live side by side after Wave 2b — the reconnect
            // policy is independent of the publish-retry policy.
            assertTrue(ctx.getBean("cdcOutboxReconnectBackOff", BackOff::class.java) is ExponentialBackOff)
            assertTrue(ctx.getBean("cdcOutboxPublishBackOff", BackOff::class.java) is ExponentialBackOff)
            assertNotNull(ctx.getBean(CdcOutboxMetrics::class.java))
            assertNotNull(ctx.getBean(SlotReaderMessageProducer::class.java))
            assertNotNull(ctx.getBean("cdcOutboxHealthIndicator", HealthIndicator::class.java))
        }
    }

    @Test
    fun `cdc-outbox-enabled false disables the entire chain`() {
        runner.withPropertyValues("cdc.outbox.enabled=false").run { ctx ->
            assertTrue(ctx.getBeansOfType(SlotReaderMessageProducer::class.java).isEmpty())
            assertTrue(ctx.getBeansOfType(CdcOutboxProperties::class.java).isEmpty())
        }
    }

    @Test
    fun `consumer-provided ConnectionProvider takes precedence over HikariCP default`() {
        runner.withUserConfiguration(CustomConnectionProvider::class.java).run { ctx ->
            val provider = ctx.getBean(ConnectionProvider::class.java)
            assertTrue(provider is StubConnectionProvider)
        }
    }

    @Configuration
    open class StubSinks {
        @Bean open fun snsProducer(): SNSProducer =
            object : SNSProducer {
                override fun <T : Any> send(
                    topicName: String,
                    message: SNSMessage<T>,
                ) = Unit
            }

        @Bean open fun sqsProducer(): SQSProducer =
            object : SQSProducer {
                override fun <T : Any> send(
                    queueName: String,
                    message: T,
                ) = Unit
            }
    }

    @Configuration
    open class NoOpLifecycle {
        /**
         * Override the auto-configured lifecycle with a no-op so the
         * application context refresh does not spawn the streaming thread.
         */
        @Bean
        open fun cdcOutboxLifecycle(): CdcOutboxLifecycle = CdcOutboxLifecycle(mockk(relaxed = true))
    }

    @Configuration
    open class CustomConnectionProvider {
        @Bean open fun connectionProvider(): ConnectionProvider = StubConnectionProvider()
    }

    class StubConnectionProvider : ConnectionProvider {
        override fun getConnection(
            url: String,
            properties: Properties,
        ): Connection = throw UnsupportedOperationException("test stub")
    }
}
