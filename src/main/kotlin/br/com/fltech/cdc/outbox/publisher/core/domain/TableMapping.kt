package br.com.fltech.cdc.outbox.publisher.core.domain

/**
 * Declarative spec for how a database table's row-level changes become
 * [OutboxEvent]s.
 *
 * Each [TableMapping] applies to exactly ONE table (matched by
 * [RowChange.table]). The mapping decides:
 *
 *  - **Capture**: which operations flow through ([capture]).
 *  - **Key**: how to derive the event id / partition key from the
 *    row columns ([key]).
 *  - **Payload**: which columns become the outbound payload and how
 *    they are renamed ([payload]).
 *  - **Event type**: the symbolic label written into the event
 *    headers ([eventType]).
 *  - **Routing**: which sink + target receives the event ([routing]).
 *
 * The shape is intentionally JSON-/YAML-friendly so it round-trips
 * cleanly through `@ConfigurationProperties` in the Spring starter.
 *
 * Implementation note: this type lives in `core/domain` and depends on
 * nothing outside the standard library — adapters reach for it but it
 * never reaches for adapters.
 */
data class TableMapping(
    /** FQ table name, e.g. `public.orders`. Compared case-sensitive. */
    val table: String,
    /** Operations to forward. Default: all three. */
    val capture: Set<RowChange.Op> = RowChange.Op.values().toSet(),
    val key: Key = Key(),
    val payload: Payload = Payload(),
    val eventType: EventType = EventType(),
    val routing: Routing,
) {

    init {
        require(table.isNotBlank()) { "table cannot be blank" }
    }

    /** How the event id / partition key is derived from row columns. */
    data class Key(
        /**
         * Columns to read from `after` (for INSERT/UPDATE) or `before`
         * (for DELETE). If empty, the mapping falls back to the
         * source's own checkpoint as the id (which is at least unique
         * but not stable across redeliveries with different LSNs).
         */
        val columns: List<String> = emptyList(),
        /**
         * Template applied over the captured columns, e.g.
         * `"order:{id}"`. `{col}` placeholders are substituted with the
         * column values' `toString()`. If null, the columns are joined
         * with `|`.
         */
        val format: String? = null,
    )

    /** How payload columns are selected, projected, and renamed. */
    data class Payload(
        /**
         * Whitelist: if non-empty, only these columns are kept.
         * Mutually exclusive with [exclude] (mapping rules enforce).
         */
        val include: List<String> = emptyList(),
        /** Blacklist applied AFTER include (or to all columns when include is empty). */
        val exclude: List<String> = emptyList(),
        /** Column-name renaming applied to the outbound key, e.g. `created_at → createdAt`. */
        val rename: Map<String, String> = emptyMap(),
    ) {
        init {
            require(include.isEmpty() || exclude.isEmpty()) {
                "payload.include and payload.exclude are mutually exclusive"
            }
        }
    }

    /** How the event type string is built. */
    data class EventType(
        /**
         * Template with `{op}` substituted by `created` (INSERT),
         * `updated` (UPDATE), or `deleted` (DELETE). Defaults to a
         * stable `{table}.{op}` form computed from the FQ table name.
         */
        val template: String = DEFAULT_TEMPLATE,
    ) {
        companion object {
            const val DEFAULT_TEMPLATE = "{table}.{op}"
        }
    }

    /**
     * Where the event goes once mapped. The [sink] is the URI-form
     * destination consumed by [br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry]
     * (e.g. `sns://orders-events`, `kafka://orders/cluster-a`).
     * [attributes] become routing-level message attributes; column
     * substitutions follow the same `{col}` syntax as [Key.format].
     */
    data class Routing(
        val sink: String,
        val attributes: Map<String, String> = emptyMap(),
    ) {
        init {
            require(sink.isNotBlank()) { "routing.sink cannot be blank" }
            // Fail at construction (i.e. application start) rather than at
            // the first mapped row. `sink` is the URI consumed by the
            // EventSinkRegistry and MUST be `scheme://target`. Without this
            // guard a misconfigured `sink: "orders-events"` would surface
            // as an IllegalArgumentException deep inside the orchestrator
            // hours after deploy. The legacy `SNS|topic` form is rejected
            // here on purpose — the legacy producer accepts it, the
            // hexagonal one expects URI form.
            require(br.com.fltech.cdc.outbox.publisher.core.domain.Routing.SCHEME_SEPARATOR in sink) {
                "routing.sink must be 'scheme://target' (got '$sink')"
            }
        }
    }
}
