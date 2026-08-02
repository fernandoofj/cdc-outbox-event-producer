package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import io.awspring.cloud.autoconfigure.core.AwsAutoConfiguration
import io.awspring.cloud.autoconfigure.core.CredentialsProviderAutoConfiguration
import io.awspring.cloud.autoconfigure.core.RegionProviderAutoConfiguration
import io.awspring.cloud.autoconfigure.sns.SnsAutoConfiguration
import io.awspring.cloud.autoconfigure.sqs.SqsAutoConfiguration
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Regression test: [CdcOutboxSinkAutoConfiguration]'s nested sink
 * configs (`SnsSinkConfig`, `SqsSinkConfig`, ...) used
 * `@ConditionalOnBean(...Template::class)` with no ordering hint
 * against the broker's own autoconfiguration. `@ConditionalOnBean`
 * only sees bean definitions registered by auto-configs processed so
 * far — without `@AutoConfigureAfter`, this class ran BEFORE Spring
 * Cloud AWS's `SnsAutoConfiguration`/`SqsAutoConfiguration` had
 * registered `SnsTemplate`/`SqsTemplate`, so every sink silently
 * failed to wire (`EventSinkRegistry` ended up with zero known
 * schemes) even with the broker starter correctly on the classpath.
 * Only caught by actually running the sample app in
 * `examples/spring-boot-postgres-sns` end-to-end — no existing unit
 * test exercised the real broker autoconfiguration classes together
 * with this one.
 */
class SinkAutoConfigurationOrderingTest {
    private val awsProps =
        arrayOf(
            "spring.cloud.aws.region.static=us-east-1",
            "spring.cloud.aws.credentials.access-key=test",
            "spring.cloud.aws.credentials.secret-key=test",
        )

    @Test
    fun `sns sink wires even when listed before SnsAutoConfiguration in configuration order`() {
        ApplicationContextRunner()
            .withConfiguration(
                // Deliberately in the "wrong" order — the fix must not
                // depend on AutoConfigurations.of()'s argument order,
                // only on the @AutoConfigureAfter metadata actually
                // taking effect.
                AutoConfigurations.of(
                    CdcOutboxSinkAutoConfiguration::class.java,
                    SnsAutoConfiguration::class.java,
                    SqsAutoConfiguration::class.java,
                    AwsAutoConfiguration::class.java,
                    CredentialsProviderAutoConfiguration::class.java,
                    RegionProviderAutoConfiguration::class.java,
                ),
            )
            .withPropertyValues(*awsProps)
            .run { ctx ->
                assertTrue(ctx.containsBean("sns"), "expected an EventSink bean named 'sns'")
                assertTrue(ctx.containsBean("sqs"), "expected an EventSink bean named 'sqs'")
                val registry = ctx.getBean(EventSinkRegistry::class.java)
                assertTrue(
                    setOf("sns", "sqs") == registry.knownSchemes(),
                    "expected {sns, sqs} in ${registry.knownSchemes()}",
                )
                assertNotNull(registry.resolve("sns"))
                assertNotNull(registry.resolve("sqs"))
            }
    }
}
