package br.com.fltech.cdc.outbox.publisher.adapter.source

import br.com.fltech.cdc.outbox.publisher.adapter.source.oracle.OracleCdcSourceStub
import br.com.fltech.cdc.outbox.publisher.adapter.source.sqlserver.SqlServerCdcSourceStub
import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.Routing
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant

class SourceStubsTest {

    private val event = OutboxEvent("e", Routing("sns", "t"), ByteArray(0), Instant.EPOCH, "ck")

    @Test
    fun `sqlserver stub throws UnsupportedOperationException on every public method`() {
        val s = SqlServerCdcSourceStub()
        assertThrows<UnsupportedOperationException> { s.open() }
        assertThrows<UnsupportedOperationException> { s.poll() }
        assertThrows<UnsupportedOperationException> { s.ack(event) }
        s.close() // close is the only no-op so resource cleanup is still safe.
    }

    @Test
    fun `oracle stub throws UnsupportedOperationException on every public method`() {
        val s = OracleCdcSourceStub()
        assertThrows<UnsupportedOperationException> { s.open() }
        assertThrows<UnsupportedOperationException> { s.poll() }
        assertThrows<UnsupportedOperationException> { s.ack(event) }
        s.close()
    }
}
