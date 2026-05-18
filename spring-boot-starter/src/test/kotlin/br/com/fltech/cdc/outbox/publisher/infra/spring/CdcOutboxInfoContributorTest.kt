package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource
import br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry
import io.mockk.every
import io.mockk.mockk
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.info.Info
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Exercises the Actuator info contributor for the three shapes a
 * production deploy can wear:
 *  - Sinks + source both present → richest detail map.
 *  - No source / no sinks → falls back to `"none"` / empty schemes
 *    without throwing (we read both via [ObjectProvider]).
 *  - Sensitive surface absence — passwords / hosts / DLQ queue names
 *    MUST NOT appear in the contributed details.
 */
class CdcOutboxInfoContributorTest {

    @Test
    fun `exposes processor, replication, source, sinks and counts`() {
        val props = CdcOutboxProperties(
            postgres = CdcOutboxProperties.Postgres(
                host = "db.prod.internal",
                username = "replica",
                password = "s3cret",
            ),
            replication = CdcOutboxProperties.Replication(
                slotName = "orders_outbox_slot",
            ),
            mappings = listOf(
                CdcOutboxProperties.MappingProps(table = "public.orders"),
                CdcOutboxProperties.MappingProps(table = "public.customers"),
            ),
        )
        val sinkRegistry = mockk<EventSinkRegistry> {
            every { knownSchemes() } returns setOf("sns", "sqs", "kafka")
        }
        val source = mockk<CdcSource>()
        val contributor = CdcOutboxInfoContributor(
            properties = props,
            sinkRegistry = providerOf(sinkRegistry),
            source = providerOf(source),
        )

        val builder = Info.Builder()
        contributor.contribute(builder)
        val details = builder.build().details["cdc-outbox"] as Map<*, *>

        assertEquals("HEXAGONAL", (details["processor"] as Map<*, *>)["kind"])
        assertEquals("orders_outbox_slot", (details["replication"] as Map<*, *>)["slot"])
        assertEquals("wal2json", (details["replication"] as Map<*, *>)["outputPlugin"])
        assertEquals(
            source.javaClass.simpleName,
            (details["source"] as Map<*, *>)["type"],
        )
        assertEquals(
            listOf("kafka", "sns", "sqs"),
            (details["sinks"] as Map<*, *>)["schemes"],
        )
        assertEquals(2, (details["mappings"] as Map<*, *>)["count"])
    }

    @Test
    fun `does NOT expose passwords, db hosts or dlq queue names`() {
        // Sentinel strings deliberately chosen so a substring leak
        // (e.g. password embedded in another field) fails the test.
        // Avoid common words like "replica" that already appear in
        // structural keys like "replication".
        val props = CdcOutboxProperties(
            postgres = CdcOutboxProperties.Postgres(
                host = "db-sentinel-leak-host",
                username = "db-sentinel-leak-user",
                password = "db-sentinel-leak-password",
            ),
            deadLetter = CdcOutboxProperties.DeadLetter(queueName = "dlq-sentinel-leak-queue"),
            dlq = CdcOutboxProperties.Dlq(
                replay = CdcOutboxProperties.Dlq.Replay(
                    enabled = true,
                    queueName = "dlq-sentinel-leak-queue",
                ),
            ),
        )
        val contributor = CdcOutboxInfoContributor(
            properties = props,
            sinkRegistry = providerOf(null),
            source = providerOf(null),
        )

        val builder = Info.Builder()
        contributor.contribute(builder)
        val rendered = builder.build().details.toString()

        assertFalse(rendered.contains("db-sentinel-leak-password"), "password must not appear")
        assertFalse(rendered.contains("db-sentinel-leak-host"), "host must not appear")
        assertFalse(rendered.contains("db-sentinel-leak-user"), "db username must not appear")
        assertFalse(rendered.contains("dlq-sentinel-leak-queue"), "queue name must not appear")
    }

    @Test
    fun `tolerates absent sink registry and absent source`() {
        val contributor = CdcOutboxInfoContributor(
            properties = CdcOutboxProperties(),
            sinkRegistry = providerOf(null),
            source = providerOf(null),
        )

        val builder = Info.Builder()
        contributor.contribute(builder)
        val details = builder.build().details["cdc-outbox"] as Map<*, *>

        assertEquals("none", (details["source"] as Map<*, *>)["type"])
        assertEquals(emptyList<String>(), (details["sinks"] as Map<*, *>)["schemes"])
        assertTrue(details["version"] is String)
    }

    /**
     * Build a real [ObjectProvider] backed by either a single instance
     * or `null` (absent bean). Using a relaxed mock here is awkward
     * because `ObjectProvider` exposes overloads that interact poorly
     * with mockk default answers; an anonymous subclass is clearer.
     */
    private fun <T : Any> providerOf(value: T?): ObjectProvider<T> = object : ObjectProvider<T> {
        override fun getObject(vararg args: Any?): T =
            value ?: error("no bean available")

        override fun getObject(): T = getObject(*emptyArray())

        override fun getIfAvailable(): T? = value

        override fun getIfUnique(): T? = value
    }
}
