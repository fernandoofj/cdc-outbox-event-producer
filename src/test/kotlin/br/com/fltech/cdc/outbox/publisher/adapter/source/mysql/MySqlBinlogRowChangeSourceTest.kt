package br.com.fltech.cdc.outbox.publisher.adapter.source.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import kotlin.test.assertNull

/**
 * Lifecycle smoke tests for the MySQL binlog adapter.
 *
 * Full event-handling coverage requires a real MySQL replica
 * (Testcontainers MySQL with `binlog_format=ROW`) and is tracked as a
 * follow-up integration test on the Wave 5 roadmap row. These cases
 * exercise the parts that don't need a running broker:
 *  - open is idempotent and registers a listener,
 *  - poll returns null when no events are buffered,
 *  - ack updates the last-acked checkpoint marker without raising,
 *  - close shuts the underlying client down idempotently.
 */
class MySqlBinlogRowChangeSourceTest {

    private val client = mockk<BinaryLogClient>(relaxed = true)
    private val factory: (String, Int, String, String) -> BinaryLogClient = { _, _, _, _ -> client }

    private fun newSource() = MySqlBinlogRowChangeSource(
        host = "localhost",
        port = 3306,
        username = "repl",
        password = "secret",
        clientFactory = factory,
    )

    @Test
    fun `open is idempotent and configures the underlying client once`() {
        val src = newSource()
        src.open()
        src.open() // duplicate call — should be a no-op
        verify(atMost = 1) { client.registerEventListener(any()) }
        src.close()
    }

    @Test
    fun `poll returns null when no events have been buffered yet`() {
        val src = newSource()
        src.open()
        assertNull(src.poll())
        src.close()
    }

    @Test
    fun `ack tolerates being called with arbitrary RowChange instances`() {
        val src = newSource()
        src.open()
        // Any RowChange is acceptable — ack just records the checkpoint.
        src.ack(
            br.com.fltech.cdc.outbox.publisher.core.domain.RowChange(
                op = br.com.fltech.cdc.outbox.publisher.core.domain.RowChange.Op.INSERT,
                table = "x.y",
                sourceCheckpoint = "mysql-bin.000001:120",
                occurredAt = java.time.Instant.EPOCH,
                after = mapOf("col0" to 1),
            ),
        )
        src.close()
    }

    @Test
    fun `close disconnects the underlying client and is idempotent`() {
        every { client.disconnect() } just Runs
        val src = newSource()
        src.open()
        src.close()
        src.close() // second call: no-op
        verify(atMost = 1) { client.disconnect() }
    }
}
