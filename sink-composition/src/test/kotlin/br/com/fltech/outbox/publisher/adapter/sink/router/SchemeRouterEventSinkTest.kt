package br.com.fltech.outbox.publisher.adapter.sink.router

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Instant

class SchemeRouterEventSinkTest {

    @Test
    fun `publish delegates to the registry's publish`() {
        val registry = mockk<EventSinkRegistry>()
        every { registry.publish(any(), any()) } just Runs

        val routing = Routing("kafka", "topic")
        val event = OutboxEvent("e1", routing, ByteArray(0), Instant.EPOCH, "ck")

        SchemeRouterEventSink(registry).publish(routing, event)

        verify(exactly = 1) { registry.publish(routing, event) }
    }
}
