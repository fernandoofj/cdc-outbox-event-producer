package br.com.fltech.cdc.outbox.publisher.adapter.checkpoint

import br.com.fltech.cdc.outbox.publisher.core.port.CheckpointStore
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

/**
 * File-system [CheckpointStore]: one JSON file per `key` under a
 * configurable directory.
 *
 * Persistence layout
 * ------------------
 *   <directory>/<sanitised-key>.json   (the canonical, current value)
 *   <directory>/<sanitised-key>.json.tmp  (only on disk during a save)
 *
 * The on-disk JSON shape is intentionally trivial:
 *
 *   {"key":"binlog:65536","value":"mysql-bin.000004:891"}
 *
 * It is hand-written (no Jackson dependency from this leaf adapter) so
 * the checkpoint can be inspected by hand without dragging the library
 * version of `ObjectMapper` into the equation. A handful of common
 * characters are escaped (quote, backslash) — checkpoint values are
 * opaque ASCII tokens (LSN strings, binlog filenames) so a full JSON
 * encoder would be overkill.
 *
 * Atomicity
 * ---------
 * `save` writes `<key>.json.tmp`, `force`s the file channel (the
 * platform's `fsync`), then `ATOMIC_MOVE`s the temp file over the
 * canonical name. On filesystems that don't support atomic moves
 * (some network mounts) the implementation falls back to a non-atomic
 * `REPLACE_EXISTING` rename and logs a WARN once — atomicity is
 * documented as a port-level invariant; operators using exotic
 * filesystems must understand the degradation.
 *
 * Corruption tolerance
 * --------------------
 * If `load` reads a file that does not decode (truncated, manually
 * edited, garbage on disk) the call logs WARN and returns `null`,
 * letting the source fall back to its natural start position rather
 * than refusing to come up. The corrupt file is left on disk so the
 * operator can investigate — overwriting it on the next `save` will
 * restore the happy path.
 *
 * Concurrency
 * -----------
 * `save` is called from the orchestrator's single thread per the
 * [br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource] /
 * [br.com.fltech.cdc.outbox.publisher.core.port.CdcSource] contract,
 * so this implementation is intentionally lock-free. If a future
 * design fans out the orchestrator across multiple threads, callers
 * must wrap the store or use distinct keys per worker.
 */
class FileCheckpointStore(
    private val directory: Path,
) : CheckpointStore {

    init {
        // Create the directory on construction so a misconfigured path
        // surfaces at startup, not on the first save call far down the
        // hot loop. `createDirectories` is a no-op if the path exists.
        Files.createDirectories(directory)
    }

    override fun load(key: String): String? {
        val file = pathFor(key)
        val raw = try {
            Files.readString(file, StandardCharsets.UTF_8)
        } catch (e: NoSuchFileException) {
            return null
        } catch (e: Exception) {
            // Treat unreadable files (permissions, locked) the same as
            // corruption — log + return null so the source can decide.
            logger.warn(
                "FileCheckpointStore failed to read checkpoint file {} ({}); treating as missing.",
                file, e.javaClass.simpleName, e,
            )
            return null
        }
        return decode(raw, key, file)
    }

    override fun save(key: String, value: String) {
        val canonical = pathFor(key)
        val tmp = canonical.resolveSibling("${canonical.fileName}.tmp")
        val payload = encode(key, value).toByteArray(StandardCharsets.UTF_8)
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
        ).use { ch ->
            ch.write(ByteBuffer.wrap(payload))
            // Force a flush to durable storage BEFORE the rename so a
            // crash between the write and the rename leaves an empty /
            // partial `.tmp` but never a damaged canonical file.
            ch.force(true)
        }
        try {
            Files.move(tmp, canonical, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (e: AtomicMoveNotSupportedException) {
            logger.warn(
                "FileCheckpointStore: filesystem at {} does not support atomic moves; " +
                    "falling back to non-atomic REPLACE_EXISTING. Crash mid-move could lose checkpoint.",
                directory,
            )
            Files.move(tmp, canonical, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private fun pathFor(key: String): Path = directory.resolve("${sanitise(key)}.json")

    private fun encode(key: String, value: String): String =
        """{"key":"${escape(key)}","value":"${escape(value)}"}"""

    private fun decode(raw: String, expectedKey: String, file: Path): String? {
        val trimmed = raw.trim()
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            logger.warn(
                "FileCheckpointStore: checkpoint file {} did not decode (not a JSON object); treating as missing.",
                file,
            )
            return null
        }
        val value = extractField(trimmed, FIELD_VALUE)
        if (value == null) {
            logger.warn(
                "FileCheckpointStore: checkpoint file {} missing 'value' field; treating as missing.",
                file,
            )
            return null
        }
        val storedKey = extractField(trimmed, FIELD_KEY)
        if (storedKey != null && storedKey != expectedKey) {
            // Sanitisation is lossy (slashes/colons collapse to '_'), so
            // an unrelated key may collide on disk. Catch it here and
            // warn — better than silently serving someone else's
            // checkpoint.
            logger.warn(
                "FileCheckpointStore: file {} stores key '{}' but caller asked for '{}'; treating as missing.",
                file, storedKey, expectedKey,
            )
            return null
        }
        return value
    }

    /**
     * Extracts a JSON string field by name. Hand-rolled rather than
     * pulling Jackson in — checkpoint files are produced by this same
     * class, the shape is known, and a stray dependency in a leaf
     * adapter would muddy the hexagonal boundary.
     */
    private fun extractField(json: String, field: String): String? {
        val needle = "\"$field\":\""
        val start = json.indexOf(needle)
        if (start < 0) return null
        val valueStart = start + needle.length
        val sb = StringBuilder()
        var i = valueStart
        while (i < json.length) {
            val c = json[i]
            when {
                c == '\\' && i + 1 < json.length -> {
                    sb.append(unescape(json[i + 1]))
                    i += 2
                }
                c == '"' -> return sb.toString()
                else -> {
                    sb.append(c)
                    i += 1
                }
            }
        }
        return null
    }

    private fun escape(s: String): String = buildString(s.length) {
        for (c in s) {
            when (c) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                else -> append(c)
            }
        }
    }

    private fun unescape(c: Char): Char = when (c) {
        'n' -> '\n'
        'r' -> '\r'
        'b' -> '\b'
        't' -> '\t'
        else -> c
    }

    /**
     * Strips characters that the file-system would refuse (slashes,
     * colons on Windows) so callers can use natural namespaced keys
     * like `"binlog:65536"` directly. Collisions across distinct keys
     * are surfaced by the `expectedKey` check in [decode].
     */
    private fun sanitise(key: String): String =
        key.map { if (it.isLetterOrDigit() || it == '-' || it == '.') it else '_' }
            .joinToString("")

    companion object {
        private val logger = LoggerFactory.getLogger(FileCheckpointStore::class.java)
        private const val FIELD_KEY = "key"
        private const val FIELD_VALUE = "value"
    }
}
