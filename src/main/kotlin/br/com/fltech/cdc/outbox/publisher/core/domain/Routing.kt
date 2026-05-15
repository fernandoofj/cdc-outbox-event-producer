package br.com.fltech.cdc.outbox.publisher.core.domain

/**
 * Identifies the destination of an [OutboxEvent].
 *
 * `scheme` is the routing namespace recognised by the
 * [br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry]
 * (`sns`, `sqs`, `kafka`, `amqp`, …). `target` is broker-specific: a
 * topic, queue, or exchange/routing-key pair. `attributes` flow through
 * to broker-level message attributes / headers / record metadata
 * (e.g. SNS message attributes, Kafka headers, AMQP `BasicProperties`).
 *
 * Routing values come from either:
 *  - a URI-like prefix emitted upstream
 *    (`sns://orders.events`, `kafka://orders/cluster-a`); see [parse],
 *  - or the legacy pipe-separated prefix used by the original
 *    `pg_logical_emit_message` protocol (`SNS|orders.events`); see
 *    [parsePrefix].
 */
data class Routing(
    val scheme: String,
    val target: String,
    val attributes: Map<String, String> = emptyMap(),
) {
    init {
        require(scheme.isNotBlank()) { "scheme cannot be blank" }
        require(target.isNotBlank()) { "target cannot be blank" }
        require(scheme == scheme.lowercase()) {
            "scheme must be lowercase (got '$scheme')"
        }
    }

    /** "scheme://target" — useful for logs and dead-letter envelopes. */
    fun asUri(): String = "$scheme://$target"

    companion object {
        const val SCHEME_SEPARATOR = "://"
        const val LEGACY_SEPARATOR = "|"

        /**
         * Parse a `scheme://target` URI. Throws when the input does not
         * contain `://`.
         */
        fun parse(uri: String): Routing {
            val idx = uri.indexOf(SCHEME_SEPARATOR)
            require(idx > 0) { "expected 'scheme://target', got '$uri'" }
            return Routing(
                scheme = uri.substring(0, idx).lowercase(),
                target = uri.substring(idx + SCHEME_SEPARATOR.length),
            )
        }

        /**
         * Parse a wal2json `pg_logical_emit_message` prefix into a
         * [Routing]. Accepts three forms:
         *
         *  - `sns://topic` / `kafka://topic` (URI form, preferred)
         *  - `SNS|topic` / `SQS|queue` (legacy form from the original
         *    project — kept for backwards compatibility)
         *  - `topic` (bare; defaults to `sns`)
         */
        fun parsePrefix(prefix: String): Routing = when {
            SCHEME_SEPARATOR in prefix -> parse(prefix)
            LEGACY_SEPARATOR in prefix -> {
                val parts = prefix.split(LEGACY_SEPARATOR, limit = 2)
                Routing(scheme = parts[0].lowercase(), target = parts[1])
            }
            else -> Routing(scheme = DEFAULT_SCHEME, target = prefix)
        }

        const val DEFAULT_SCHEME = "sns"
    }
}
