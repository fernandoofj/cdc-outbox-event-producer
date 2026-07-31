package br.com.fltech.outbox.publisher.core.application

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.domain.RowChange
import br.com.fltech.outbox.publisher.core.domain.TableMapping
import br.com.fltech.outbox.publisher.core.port.MappingRules

/**
 * Reference [MappingRules] implementation driven by a static list of
 * [TableMapping]s.
 *
 * Resolution is by exact [TableMapping.table] match. The mapping owns:
 *  - **Key derivation** — `{col}` template over the operation-relevant
 *    snapshot ([RowChange.after] for INSERT/UPDATE, [RowChange.before]
 *    for DELETE).
 *  - **Payload projection** — include / exclude / rename. Renames keep
 *    the original value but rewrite the key.
 *  - **Event-type formatting** — `{table}.{op}` by default;
 *    `{op}` resolves to `created` / `updated` / `deleted`.
 *  - **Routing materialisation** — `{col}` substitution in attributes.
 *
 * Serialisation of payload values into the byte array delegated to
 * [serializer]; the default uses each value's `toString()`. Spring
 * starter consumers pass in a Jackson-backed serializer.
 */
class DefaultMappingRules(
    private val mappings: List<TableMapping>,
    private val serializer: (Map<String, Any?>) -> ByteArray = DEFAULT_SERIALIZER,
) : MappingRules {
    private val byTable: Map<String, TableMapping> = mappings.associateBy { it.table }

    init {
        require(byTable.size == mappings.size) {
            val dups = mappings.groupBy { it.table }.filter { it.value.size > 1 }.keys
            "duplicate TableMapping entries for tables: $dups"
        }
    }

    // ReturnCount: 3 explicit returns map to 3 filter steps (no table
    // mapping, op excluded by capture set, mapped event). Each guard
    // is a distinct cause for the caller to register the row as
    // ignored — folding them loses that signal.
    @Suppress("ReturnCount")
    override fun map(rowChange: RowChange): OutboxEvent? {
        val mapping = byTable[rowChange.table] ?: return null
        if (rowChange.op !in mapping.capture) return null

        val operationSnapshot =
            when (rowChange.op) {
                RowChange.Op.INSERT, RowChange.Op.UPDATE -> rowChange.after
                RowChange.Op.DELETE -> rowChange.before
            }
        val id = deriveKey(mapping.key, operationSnapshot, rowChange.sourceCheckpoint)
        val payload = projectPayload(mapping.payload, operationSnapshot)
        val eventType = formatEventType(mapping.eventType.template, rowChange)
        val attributes =
            mapping.routing.attributes.mapValues { (_, v) ->
                substitute(v, operationSnapshot)
            }
        val routing = Routing.parse(mapping.routing.sink).copy(attributes = attributes)

        return OutboxEvent(
            id = id,
            routing = routing,
            payload = serializer(payload),
            occurredAt = rowChange.occurredAt,
            sourceCheckpoint = rowChange.sourceCheckpoint,
            headers = mapOf(HEADER_EVENT_TYPE to eventType),
        )
    }

    private fun deriveKey(
        keySpec: TableMapping.Key,
        snapshot: Map<String, Any?>,
        fallback: String,
    ): String {
        if (keySpec.columns.isEmpty()) return fallback
        return if (keySpec.format != null) {
            substitute(keySpec.format, snapshot)
        } else {
            keySpec.columns.joinToString(KEY_DEFAULT_SEPARATOR) { col ->
                snapshot[col]?.toString().orEmpty()
            }
        }
    }

    private fun projectPayload(
        spec: TableMapping.Payload,
        snapshot: Map<String, Any?>,
    ): Map<String, Any?> {
        val filtered =
            when {
                spec.include.isNotEmpty() -> snapshot.filterKeys { it in spec.include }
                spec.exclude.isNotEmpty() -> snapshot.filterKeys { it !in spec.exclude }
                else -> snapshot
            }
        if (spec.rename.isEmpty()) return filtered
        return filtered.mapKeys { (k, _) -> spec.rename[k] ?: k }
    }

    private fun formatEventType(
        template: String,
        rowChange: RowChange,
    ): String {
        val op =
            when (rowChange.op) {
                RowChange.Op.INSERT -> "created"
                RowChange.Op.UPDATE -> "updated"
                RowChange.Op.DELETE -> "deleted"
            }
        return template
            .replace("{table}", rowChange.table)
            .replace("{op}", op)
    }

    /**
     * Substitute `{column}` placeholders with the matching value's
     * `toString()`. Missing columns render as an empty string; the
     * mapping caller is responsible for not relying on absent columns.
     */
    private fun substitute(
        template: String,
        snapshot: Map<String, Any?>,
    ): String {
        if ('{' !in template) return template
        return PLACEHOLDER_REGEX.replace(template) { match ->
            snapshot[match.groupValues[1]]?.toString().orEmpty()
        }
    }

    companion object {
        const val HEADER_EVENT_TYPE = "eventType"
        const val KEY_DEFAULT_SEPARATOR = "|"
        private val PLACEHOLDER_REGEX = Regex("\\{([A-Za-z_][A-Za-z0-9_]*)}")

        /** Best-effort fallback: serialise map entries as `k=v` lines. */
        val DEFAULT_SERIALIZER: (Map<String, Any?>) -> ByteArray = { map ->
            map.entries.joinToString("\n") { (k, v) -> "$k=${v ?: ""}" }
                .toByteArray(Charsets.UTF_8)
        }
    }
}
