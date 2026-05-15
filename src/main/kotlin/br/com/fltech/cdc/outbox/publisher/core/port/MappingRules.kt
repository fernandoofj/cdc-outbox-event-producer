package br.com.fltech.cdc.outbox.publisher.core.port

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.domain.TableMapping

/**
 * Projects a row-level [RowChange] into an [OutboxEvent], consulting
 * the user-provided [TableMapping]s.
 *
 * Returns `null` when:
 *  - there is no mapping for [RowChange.table], OR
 *  - the mapping exists but its `capture` set excludes the row's op.
 *
 * Implementations MUST be pure: same input → same output. Stateful
 * concerns (counters, retries) live in the orchestrator.
 */
fun interface MappingRules {
    fun map(rowChange: RowChange): OutboxEvent?

    companion object {
        /**
         * An empty rules set that always returns `null` — the orchestrator
         * will then drop unmapped row events. Useful as a default when the
         * consumer hasn't configured any mappings yet.
         */
        val EMPTY: MappingRules = MappingRules { _ -> null }
    }
}

/** Thrown by [MappingRules] implementations when the input is malformed. */
class MappingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
