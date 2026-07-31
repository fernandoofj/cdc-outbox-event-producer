package br.com.fltech.outbox.publisher.deadletter

import br.com.fltech.outbox.publisher.replication.model.MessageChange
import io.awspring.cloud.sqs.operations.SqsTemplate
import org.postgresql.replication.LogSequenceNumber
import java.time.Instant

/**
 * Dead-letter sink that publishes failed messages to an SQS queue.
 *
 * The envelope is a flat map (JSON-serialisable by SCA's default
 * converter) so the queue payload remains queryable from the AWS
 * console / `awscli` without a custom deserialiser. Fields:
 *  - `originalPrefix`: prefix as emitted by `pg_logical_emit_message`.
 *  - `lsn`: WAL LSN where the failure was captured (string form).
 *  - `content`: the original payload (still as a string — we don't
 *    re-parse it, in case the parse error is what's causing the
 *    failure).
 *  - `failureType`: simple class name of the last exception.
 *  - `failureMessage`: exception message, never null.
 *  - `deadLetteredAt`: ISO-8601 UTC timestamp from [Instant.now].
 */
class SqsDeadLetterSink(
    private val sqsTemplate: SqsTemplate,
    private val queueName: String,
) : DeadLetterSink {
    override fun send(
        lsn: LogSequenceNumber,
        messageChange: MessageChange,
        lastException: Throwable,
    ) {
        val envelope =
            linkedMapOf(
                "originalPrefix" to messageChange.prefix,
                // `asString()` emits the canonical Postgres `X/X` hex pair
                // (e.g. `0/16E8198`); `toString()` wraps it in `LSN{...}`,
                // which is fine for logs but a poor key for queries.
                "lsn" to lsn.asString(),
                "content" to messageChange.content,
                "failureType" to lastException.javaClass.simpleName,
                "failureMessage" to (lastException.message ?: ""),
                "deadLetteredAt" to Instant.now().toString(),
            )
        sqsTemplate.send { it.queue(queueName).payload(envelope) }
    }
}
