package br.com.fltech.outbox.publisher.adapter.source.mysql

import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import javax.sql.DataSource

class MySqlOutboxTableCdcSourceTest {
    private val dataSource = mockk<DataSource>(relaxed = true)

    @Test
    fun `constructor rejects table names that escape the conservative identifier regex`() {
        val badNames =
            listOf(
                "outbox; DROP TABLE users",
                "outbox_events`; --",
                "outbox events",
                "1outbox",
                "",
                "outbox-events",
                "outbox.events",
            )
        badNames.forEach { name ->
            assertThrows<IllegalArgumentException>("expected reject for '$name'") {
                MySqlOutboxTableCdcSource(dataSource, tableName = name)
            }
        }
    }

    @Test
    fun `constructor accepts plain snake_case identifiers`() {
        listOf("outbox_events", "OutboxEvents", "_outbox", "t1", "T_42").forEach { name ->
            // No exception expected.
            MySqlOutboxTableCdcSource(dataSource, tableName = name)
        }
    }

    @Test
    fun `batchSize must be within 1 to 1000`() {
        assertThrows<IllegalArgumentException> { MySqlOutboxTableCdcSource(dataSource, batchSize = 0) }
        assertThrows<IllegalArgumentException> { MySqlOutboxTableCdcSource(dataSource, batchSize = -1) }
        assertThrows<IllegalArgumentException> { MySqlOutboxTableCdcSource(dataSource, batchSize = 1001) }
        // boundaries OK
        MySqlOutboxTableCdcSource(dataSource, batchSize = 1)
        MySqlOutboxTableCdcSource(dataSource, batchSize = 1000)
    }
}
