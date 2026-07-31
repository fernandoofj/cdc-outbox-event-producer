package br.com.fltech.outbox.publisher.adapter.dlq.replay

/**
 * Port over the dead-letter backend. Today only SQS is implemented
 * ([SqsDlqReader]); the port exists so a future Kafka or Rabbit DLQ
 * reader can be dropped in without touching [DlqReplayService].
 *
 * Implementations must be safe to call from a single operator-driven
 * thread. Concurrent invocations are not supported.
 */
interface DlqReader {
    /**
     * Returns up to [max] envelopes from the DLQ without consuming
     * them. Each entry carries a [Handle] the operator can later use
     * to [delete] or replay-and-delete. Implementations MUST make
     * the message visible again after the peek so other consumers /
     * subsequent peeks can see it.
     */
    fun peek(max: Int): List<Message>

    /**
     * Permanently removes the message identified by [handle] from
     * the DLQ. Used both by replay-on-success and abandon flows.
     * Throws when the handle is unknown / expired.
     */
    fun delete(handle: Handle)

    /**
     * Aggregate counters across the DLQ — operator-facing stats.
     * Implementations may return approximate values when the backend
     * does not expose exact counts cheaply.
     */
    fun stats(): Stats

    data class Message(val handle: Handle, val envelope: DlqEnvelope)

    data class Handle(val value: String)

    data class Stats(
        val approximateMessageCount: Long,
        val approximateMessageNotVisibleCount: Long,
    )
}
