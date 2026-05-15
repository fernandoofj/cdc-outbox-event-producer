package br.com.fltech.cdc.outbox.publisher.core.application

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.domain.TableMapping
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DefaultMappingRulesTest {

    private val now = Instant.parse("2026-05-15T00:00:00Z")

    private fun mappingOrders(
        capture: Set<RowChange.Op> = RowChange.Op.values().toSet(),
        keyColumns: List<String> = listOf("id"),
        keyFormat: String? = null,
        include: List<String> = emptyList(),
        exclude: List<String> = emptyList(),
        rename: Map<String, String> = emptyMap(),
        eventTemplate: String = "{table}.{op}",
        sink: String = "sns://orders-events",
        attributes: Map<String, String> = emptyMap(),
    ) = TableMapping(
        table = "public.orders",
        capture = capture,
        key = TableMapping.Key(columns = keyColumns, format = keyFormat),
        payload = TableMapping.Payload(include = include, exclude = exclude, rename = rename),
        eventType = TableMapping.EventType(template = eventTemplate),
        routing = TableMapping.Routing(sink = sink, attributes = attributes),
    )

    private fun row(
        op: RowChange.Op,
        table: String = "public.orders",
        before: Map<String, Any?> = emptyMap(),
        after: Map<String, Any?> = emptyMap(),
        checkpoint: String = "0/16E8198",
    ) = RowChange(
        op = op, table = table, sourceCheckpoint = checkpoint, occurredAt = now,
        before = before, after = after,
    )

    @Test
    fun `map returns null when there is no mapping for the table`() {
        val rules = DefaultMappingRules(listOf(mappingOrders()))
        assertNull(rules.map(row(RowChange.Op.INSERT, table = "public.customers", after = mapOf("id" to 1))))
    }

    @Test
    fun `map returns null when the row's op is not in the mapping's capture set`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(capture = setOf(RowChange.Op.INSERT))))
        assertNull(rules.map(row(RowChange.Op.UPDATE, after = mapOf("id" to 1))))
    }

    @Test
    fun `INSERT uses 'after' snapshot for key derivation and payload projection`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(
            keyFormat = "order:{id}",
            include = listOf("id", "status"),
        )))
        val event = rules.map(row(
            op = RowChange.Op.INSERT,
            after = mapOf("id" to 42, "status" to "PENDING", "total_cents" to 999),
        ))
        assertNotNull(event)
        assertEquals("order:42", event.id)
        val payload = event.payloadAsString
        assertEquals(setOf("id=42", "status=PENDING"), payload.split("\n").toSet())
        assertEquals("sns", event.routing.scheme)
        assertEquals("orders-events", event.routing.target)
    }

    @Test
    fun `DELETE uses 'before' snapshot for key derivation`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(keyFormat = "{id}")))
        val event = rules.map(row(
            op = RowChange.Op.DELETE,
            before = mapOf("id" to 7),
        ))
        assertNotNull(event)
        assertEquals("7", event.id)
        assertEquals("public.orders.deleted", event.headers["eventType"])
    }

    @Test
    fun `eventType template substitutes table and op`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(
            eventTemplate = "orders.{op}",
        )))
        assertEquals(
            "orders.created",
            rules.map(row(RowChange.Op.INSERT, after = mapOf("id" to 1)))?.headers?.get("eventType"),
        )
        assertEquals(
            "orders.updated",
            rules.map(row(RowChange.Op.UPDATE, after = mapOf("id" to 1)))?.headers?.get("eventType"),
        )
        assertEquals(
            "orders.deleted",
            rules.map(row(RowChange.Op.DELETE, before = mapOf("id" to 1)))?.headers?.get("eventType"),
        )
    }

    @Test
    fun `payload include and exclude are mutually exclusive at construction`() {
        assertThrows<IllegalArgumentException> {
            TableMapping.Payload(include = listOf("a"), exclude = listOf("b"))
        }
    }

    @Test
    fun `payload exclude drops the listed columns and keeps the rest`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(
            exclude = listOf("password_hash"),
        )))
        val event = rules.map(row(
            op = RowChange.Op.INSERT,
            after = mapOf("id" to 1, "name" to "Ada", "password_hash" to "secret"),
        ))
        assertNotNull(event)
        val payload = event.payloadAsString
        assertEquals(setOf("id=1", "name=Ada"), payload.split("\n").toSet())
    }

    @Test
    fun `payload rename rewrites the outbound keys while keeping values`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(
            rename = mapOf("total_cents" to "totalCents", "created_at" to "createdAt"),
        )))
        val event = rules.map(row(
            op = RowChange.Op.INSERT,
            after = mapOf("id" to 1, "total_cents" to 999, "created_at" to "2026-01-01"),
        ))
        assertNotNull(event)
        val keys = event.payloadAsString.split("\n").map { it.substringBefore("=") }.toSet()
        assertEquals(setOf("id", "totalCents", "createdAt"), keys)
    }

    @Test
    fun `routing attributes substitute column placeholders`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(
            attributes = mapOf("tenant" to "{tenant_id}", "constant" to "fixed"),
        )))
        val event = rules.map(row(
            op = RowChange.Op.INSERT,
            after = mapOf("id" to 1, "tenant_id" to "acme"),
        ))
        assertNotNull(event)
        assertEquals(mapOf("tenant" to "acme", "constant" to "fixed"), event.routing.attributes)
    }

    @Test
    fun `default key format joins columns with pipe when no template is given`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(
            keyColumns = listOf("tenant_id", "id"),
            keyFormat = null,
        )))
        val event = rules.map(row(
            op = RowChange.Op.INSERT,
            after = mapOf("tenant_id" to "acme", "id" to 42),
        ))
        assertNotNull(event)
        assertEquals("acme|42", event.id)
    }

    @Test
    fun `empty key columns fall back to the sourceCheckpoint`() {
        val rules = DefaultMappingRules(listOf(mappingOrders(keyColumns = emptyList())))
        val event = rules.map(row(
            op = RowChange.Op.INSERT, after = mapOf("id" to 1), checkpoint = "0/CAFE",
        ))
        assertEquals("0/CAFE", event?.id)
    }

    @Test
    fun `duplicate mapping entries for the same table are rejected at construction`() {
        assertThrows<IllegalArgumentException> {
            DefaultMappingRules(listOf(mappingOrders(), mappingOrders()))
        }
    }
}
