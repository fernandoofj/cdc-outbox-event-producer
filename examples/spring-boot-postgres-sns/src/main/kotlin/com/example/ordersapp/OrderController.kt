package com.example.ordersapp

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

data class CreateOrderRequest(val totalCents: Long)

data class OrderPlacedEvent(
    val eventType: String,
    val domainId: String,
    val payload: Map<String, Any?>,
)

/**
 * The whole point of this sample: [createOrder] never talks to SNS. It
 * inserts a row and emits a WAL message in one transaction; the
 * `cdc-outbox-spring-boot-starter` auto-configuration wired into this
 * same process (see the README) reads the replication slot and does
 * the actual publish, with retry and dead-lettering the app code
 * never sees.
 */
@RestController
class OrderController(
    private val orderRepository: OrderRepository,
    private val objectMapper: ObjectMapper,
) {
    @PostMapping("/orders")
    @Transactional
    fun createOrder(
        @RequestBody request: CreateOrderRequest,
    ): Order {
        val order = orderRepository.save(Order(status = "PLACED", totalCents = request.totalCents))

        val event =
            OrderPlacedEvent(
                eventType = "OrderPlaced",
                domainId = "order:${order.id}",
                payload =
                    mapOf(
                        "id" to order.id,
                        "status" to order.status,
                        "totalCents" to order.totalCents,
                    ),
            )

        orderRepository.emitLogicalMessage(
            transactional = true,
            // SNS topic names may only contain ASCII letters, numbers,
            // underscores, and hyphens — no dots.
            prefix = "sns://orders-events",
            content = objectMapper.writeValueAsString(event),
        )

        return order
    }
}
