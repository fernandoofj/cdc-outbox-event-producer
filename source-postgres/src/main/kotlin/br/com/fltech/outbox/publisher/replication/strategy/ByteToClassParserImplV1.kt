package br.com.fltech.outbox.publisher.replication.strategy

import br.com.fltech.outbox.publisher.replication.model.Change
import br.com.fltech.outbox.publisher.replication.model.DeleteChange
import br.com.fltech.outbox.publisher.replication.model.InsertChange
import br.com.fltech.outbox.publisher.replication.model.MessageChange
import br.com.fltech.outbox.publisher.replication.model.SlotMessageV1
import br.com.fltech.outbox.publisher.replication.model.UpdateChange
import br.com.fltech.outbox.publisher.replication.model.Wal2JsonColumn
import br.com.fltech.outbox.publisher.replication.model.v1.V1OldKeys
import br.com.fltech.outbox.publisher.replication.model.v1.V1RowRecord
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.ByteBuffer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Decodes a wal2json `format-version=1` slot message. V1 batches every
 * WAL operation of a transaction under a single wrapper (`change`
 * array), so one call returns N [Change]s.
 *
 * The wire shape differs from V2 (parallel arrays per row record
 * rather than a `columns: [{name,type,value}]` array, with replica
 * identity nested under `oldkeys`), so this parser binds the raw V1
 * fields into [V1RowRecord] and translates each entry into the
 * canonical [InsertChange] / [UpdateChange] / [DeleteChange] /
 * [MessageChange] subtypes that downstream consumers — notably
 * [br.com.fltech.outbox.publisher.adapter.source.postgres.PgWalRowChangeSource]
 * and the legacy `pg_logical_emit_message` path on
 * [br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer] —
 * already understand.
 *
 * `lsn` lives on the V1 wrapper (`nextlsn`), not on each child record.
 * The parser copies it down to every translated child so per-record
 * LSN resolution downstream behaves the same way as V2.
 *
 * Records whose `kind` is not one of `insert` / `update` / `delete` /
 * `message` (e.g. `truncate`, future wal2json additions) are surfaced
 * as a generic [Change] with the original `kind` preserved; consumers
 * filter them out the same way they ignore [Change] today. Malformed
 * row records — missing or mismatched-length column arrays — degrade
 * to null `columns` / `identity` with a WARN log so the slot keeps
 * draining instead of head-of-line blocking the whole transaction.
 */
@Component
class ByteToClassParserImplV1(
    private val defaultMapper: ObjectMapper,
) : ByteToClassParser {

    override fun parse(byteBufferMessage: ByteBuffer): List<Change> {
        val byteArray = ByteArray(byteBufferMessage.remaining())
        byteBufferMessage.get(byteArray)
        val jsonString = String(byteArray, Charsets.UTF_8)
        val slotMessage = defaultMapper.readValue(jsonString, SlotMessageV1::class.java)
        val wrapperLsn = slotMessage.nextLsn
        return slotMessage.changes.map { record -> translate(record, wrapperLsn) }
    }

    private fun translate(record: V1RowRecord, wrapperLsn: String?): Change =
        when (record.kind.lowercase()) {
            KIND_INSERT -> InsertChange(
                kindInput = KIND_INSERT,
                schema = record.schema,
                table = record.table,
                lsn = wrapperLsn,
                columns = zipColumns(record),
            )
            KIND_UPDATE -> UpdateChange(
                kindInput = KIND_UPDATE,
                schema = record.schema,
                table = record.table,
                lsn = wrapperLsn,
                columns = zipColumns(record),
                identity = zipIdentity(record.oldKeys),
            )
            KIND_DELETE -> DeleteChange(
                kindInput = KIND_DELETE,
                schema = record.schema,
                table = record.table,
                lsn = wrapperLsn,
                identity = zipIdentity(record.oldKeys),
            )
            KIND_MESSAGE -> translateMessage(record, wrapperLsn)
            else -> Change(record.kind)
        }

    /**
     * Translates a V1 `pg_logical_emit_message` record into the
     * canonical [MessageChange]. The required fields (`prefix`,
     * `content`, `transactional`) are validated up-front; a record
     * missing any of them is logged at WARN and surfaced as a generic
     * [Change] so the slot keeps draining instead of head-of-line
     * blocking on a malformed payload.
     */
    private fun translateMessage(record: V1RowRecord, wrapperLsn: String?): Change {
        val prefix = record.prefix
        val content = record.content
        val transactional = record.transactional
        if (prefix == null || content == null || transactional == null) {
            logger.warn(
                "wal2json V1 message record dropped: missing required fields " +
                    "(prefix={}, content={}, transactional={}); degrading to generic Change.",
                prefix != null, content != null, transactional != null,
            )
            return Change(KIND_MESSAGE)
        }
        return MessageChange(
            kindInput = KIND_MESSAGE,
            transactional = transactional,
            prefix = prefix,
            content = content,
            lsn = wrapperLsn,
        )
    }

    /**
     * Zips V1's parallel post-image arrays (`columnnames` /
     * `columntypes` / `columnvalues`) into V2's
     * `List<Wal2JsonColumn>` representation so the in-memory model is
     * identical across plugin versions.
     *
     * Returns null when the post-image is absent (legitimate on
     * non-row-mutating records sharing the same `kind` dispatch, or on
     * the V1 `truncate` shape some plugin builds emit alongside row
     * changes) so the caller can leave `columns = null` rather than
     * fabricate an empty list. Mismatched array lengths log at WARN
     * and degrade to null — the slot keeps draining rather than
     * head-of-line blocking on a malformed record.
     */
    // ReturnCount: missing names and missing values are different
    // wal2json shapes (truncate, message-only, malformed) — each
    // null guard carries operational meaning the caller can register.
    @Suppress("ReturnCount")
    private fun zipColumns(record: V1RowRecord): List<Wal2JsonColumn>? {
        val names = record.columnNames ?: return null
        val values = record.columnValues ?: return null
        val types = record.columnTypes
        return zipParallelArrays(
            names = names,
            types = types,
            values = values,
            recordContext = "${record.schema}.${record.table} kind=${record.kind}",
            arrayLabel = "columnnames/columntypes/columnvalues",
        )
    }

    /**
     * Zips V1's nested `oldkeys` parallel arrays into V2's
     * `List<Wal2JsonColumn>` representation for the replica-identity
     * projection. Same null-tolerance and length-mismatch semantics as
     * [zipColumns].
     */
    // ReturnCount: same guard rationale as [zipColumns] — 4 null
    // shapes (no oldkeys block, no keyNames, no keyValues, mismatched).
    @Suppress("ReturnCount")
    private fun zipIdentity(oldKeys: V1OldKeys?): List<Wal2JsonColumn>? {
        if (oldKeys == null) return null
        val names = oldKeys.keyNames ?: return null
        val values = oldKeys.keyValues ?: return null
        val types = oldKeys.keyTypes
        return zipParallelArrays(
            names = names,
            types = types,
            values = values,
            recordContext = "oldkeys",
            arrayLabel = "keynames/keytypes/keyvalues",
        )
    }

    private fun zipParallelArrays(
        names: List<String>,
        types: List<String?>?,
        values: List<Any?>,
        recordContext: String,
        arrayLabel: String,
    ): List<Wal2JsonColumn>? {
        if (names.size != values.size) {
            logger.warn(
                "wal2json V1 record [{}] has mismatched array lengths " +
                    "({} names vs {} values, fields={}); degrading to null.",
                recordContext, names.size, values.size, arrayLabel,
            )
            return null
        }
        if (types != null && types.size != names.size) {
            logger.warn(
                "wal2json V1 record [{}] has mismatched type-array length " +
                    "({} names vs {} types, fields={}); types dropped, values kept.",
                recordContext, names.size, types.size, arrayLabel,
            )
        }
        return names.mapIndexed { index, name ->
            Wal2JsonColumn(
                name = name,
                type = types?.takeIf { it.size == names.size }?.get(index),
                value = values[index],
            )
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ByteToClassParserImplV1::class.java)
        private const val KIND_INSERT = "insert"
        private const val KIND_UPDATE = "update"
        private const val KIND_DELETE = "delete"
        private const val KIND_MESSAGE = "message"
    }
}
