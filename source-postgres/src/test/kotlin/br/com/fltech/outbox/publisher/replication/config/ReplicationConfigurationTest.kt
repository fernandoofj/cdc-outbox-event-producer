package br.com.fltech.outbox.publisher.replication.config

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Ensures the slot options sent to Postgres include `include-lsn=true` by
 * default — the toggle that, paired with the parser change in Wave 1.5,
 * closes the at-least-once race.
 */
class ReplicationConfigurationTest {

    @Test
    fun `default slot options enable include-lsn so wal2json emits per-record LSNs`() {
        val options = ReplicationConfiguration(slotName = "any_slot").getSlotOptions()

        assertEquals("true", options.getProperty("include-lsn"))
        assertEquals("true", options.getProperty("include-xids"))
        assertEquals("2", options.getProperty("format-version"))
    }

    @Test
    fun `explicit includeLsn false is forwarded to the plugin`() {
        val options = ReplicationConfiguration(
            slotName = "any_slot",
            includeLsn = false,
        ).getSlotOptions()

        assertEquals("false", options.getProperty("include-lsn"))
    }
}
