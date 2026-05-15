package br.com.fltech.cdc.outbox.publisher.deadletter

import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import org.postgresql.replication.LogSequenceNumber

/**
 * Receives messages that exhausted their publish retries.
 *
 * Implementations MUST be idempotent: the producer treats the
 * dead-letter send as the operation that "owns" the message and only
 * advances the slot LSN once `send` returns normally. If the sink
 * itself fails, the producer keeps the slot held back so the next
 * iteration retries delivery (which may succeed) or dead-letters
 * (which lands here again).
 */
interface DeadLetterSink {
    /**
     * Forward [messageChange] to the dead-letter destination annotated
     * with the captured [lsn] and the last sink-publish failure
     * [lastException]. Throwing means the dead-letter itself failed;
     * the producer will react accordingly.
     */
    fun send(lsn: LogSequenceNumber, messageChange: MessageChange, lastException: Throwable)
}
