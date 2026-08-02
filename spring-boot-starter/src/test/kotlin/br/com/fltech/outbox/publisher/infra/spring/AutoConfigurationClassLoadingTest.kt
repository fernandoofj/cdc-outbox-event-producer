package br.com.fltech.outbox.publisher.infra.spring

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.io.File
import java.net.URLClassLoader
import kotlin.test.assertTrue

/**
 * Low-level reflection regression coverage for the "outer
 * `@AutoConfiguration` class has an unconditional `@Bean` method
 * referencing an optional module's type" bug class fixed in Round 25 —
 * [OptionalModuleAbsentTest]'s `FilteredClassLoader`-based tests do
 * NOT actually catch this for [CdcOutboxHexagonalAutoConfiguration]'s
 * legacy/lag-probe/source-postgres isolation fixes. `FilteredClassLoader`
 * only intercepts explicit `Class.forName` lookups — the mechanism
 * `@ConditionalOnClass` uses — it does not stop the JVM from reflecting
 * over a class that was already loaded by the real (unfiltered) test
 * classloader. Since the declaring class here is loaded once, up
 * front, by the normal classloader, `getDeclaredMethods()` resolves
 * every parameter/return type through THAT classloader regardless of
 * what `ApplicationContextRunner.withClassLoader(...)` is set to.
 * Confirmed by mutating the fixed code back to its pre-fix shape and
 * re-running `OptionalModuleAbsentTest`: it stayed green.
 *
 * Reproducing the real crash requires the declaring class to be loaded
 * fresh through a classloader that genuinely lacks the optional
 * module's compiled output — which is exactly what a consumer's
 * runtime classpath looks like when they didn't declare that
 * dependency. That's what these tests do: build a [URLClassLoader]
 * over the real test classpath minus the target module's jar, load
 * the declaring class through it, and call `getDeclaredMethods()` —
 * the exact call that threw `NoClassDefFoundError` before the fix,
 * because the JVM eagerly resolves every declared method's
 * parameter/return types to build the `Method[]` array, not just the
 * ones whose Spring conditions would separately evaluate true.
 */
class AutoConfigurationClassLoadingTest {
    @Test
    fun `CdcOutboxHexagonalAutoConfiguration reflects cleanly without the legacy module`() {
        assertReflectsCleanly(CdcOutboxHexagonalAutoConfiguration::class.java.name, "/legacy/build/")
    }

    @Test
    fun `CdcOutboxHexagonalAutoConfiguration reflects cleanly without the lag-probes module`() {
        assertReflectsCleanly(CdcOutboxHexagonalAutoConfiguration::class.java.name, "/lag-probes/build/")
    }

    @Test
    fun `CdcOutboxHexagonalAutoConfiguration reflects cleanly without the source-postgres module`() {
        assertReflectsCleanly(CdcOutboxHexagonalAutoConfiguration::class.java.name, "/source-postgres/build/")
    }

    @Test
    fun `CdcOutboxAutoConfiguration reflects cleanly without the legacy module`() {
        assertReflectsCleanly(CdcOutboxAutoConfiguration::class.java.name, "/legacy/build/")
    }

    @Test
    fun `CdcOutboxAutoConfiguration reflects cleanly without the source-postgres module`() {
        assertReflectsCleanly(CdcOutboxAutoConfiguration::class.java.name, "/source-postgres/build/")
    }

    @Test
    fun `CdcOutboxSinkAutoConfiguration reflects cleanly without any cdc-outbox sink adapter module`() {
        // CdcOutboxSinkAutoConfiguration's own bean methods never
        // reference an adapter-module type directly (each is isolated
        // in its own nested SnsSinkConfig/SqsSinkConfig/... since
        // Wave 4) — this is a baseline sanity check that stripping
        // every cdc-outbox sink-adapter jar at once still leaves the
        // outer class reflectable. It does NOT strip the underlying
        // broker client libraries (spring-kafka, spring-rabbit,
        // spring-cloud-aws) themselves, which this class's own bean
        // signatures never reference either way.
        assertReflectsCleanly(
            CdcOutboxSinkAutoConfiguration::class.java.name,
            "/sink-aws/build/",
            "/sink-kafka/build/",
            "/sink-rabbitmq/build/",
        )
    }

    /**
     * Loads [className] through a classloader whose URLs exclude any
     * classpath entry matching one of [excludedPathFragments], then
     * calls `getDeclaredMethods()`.
     */
    private fun assertReflectsCleanly(
        className: String,
        vararg excludedPathFragments: String,
    ) {
        val fullClasspath = System.getProperty("java.class.path").split(File.pathSeparator)
        val trimmedClasspath = fullClasspath.filterNot { entry -> excludedPathFragments.any { entry.contains(it) } }
        assertTrue(
            trimmedClasspath.size < fullClasspath.size,
            "expected ${excludedPathFragments.toList()} to match at least one classpath entry — " +
                "otherwise this test passes vacuously without excluding anything",
        )
        val urls = trimmedClasspath.map { File(it).toURI().toURL() }.toTypedArray()
        val isolatedLoader = URLClassLoader(urls, ClassLoader.getPlatformClassLoader())
        val isolatedClass = Class.forName(className, false, isolatedLoader)
        assertDoesNotThrow { isolatedClass.declaredMethods }
    }
}
