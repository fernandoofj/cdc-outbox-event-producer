package br.com.fltech.outbox.publisher.adapter.checkpoint

import br.com.fltech.outbox.publisher.core.port.CheckpointStore
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import org.slf4j.LoggerFactory
import java.io.IOException
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
 * [br.com.fltech.outbox.publisher.core.port.RowChangeSource] /
 * [br.com.fltech.outbox.publisher.core.port.CdcSource] contract,
 * so this implementation is intentionally lock-free. If a future
 * design fans out the orchestrator across multiple threads, callers
 * must wrap the store or use distinct keys per worker.
 *
 * Crash-recovery sweep
 * --------------------
 * If the process crashed between the `force` and the `ATOMIC_MOVE` in
 * a prior `save`, an orphan `<key>.json.tmp` is left on disk. The
 * canonical `<key>.json` either still holds the previous good value
 * (the rename never happened) or already holds the new one (the
 * rename succeeded but a second crash interrupted the cleanup of an
 * older sibling). Either way the orphan is stale; the producer
 * resumes from `<key>.json`. The sweep runs eagerly in the
 * constructor — the bean is short-lived (one per process) so the FS
 * scan happens exactly once at startup, and tests can observe the
 * post-sweep state immediately without an explicit `init()` hop. The
 * sweep is idempotent: running it twice is a no-op because the
 * second pass finds no `.tmp` siblings.
 */
class FileCheckpointStore(
    private val directory: Path,
    private val metrics: CdcOutboxMetrics = CdcOutboxMetrics.noop(),
) : CheckpointStore {

    init {
        // Create the directory on construction so a misconfigured path
        // surfaces at startup, not on the first save call far down the
        // hot loop. `createDirectories` is a no-op if the path exists.
        Files.createDirectories(directory)
        sweepOrphans()
    }

    /**
     * Deletes orphan `<key>.json.tmp` files left behind by a crash
     * between `force` and the final `ATOMIC_MOVE` of a prior [save].
     *
     * Operators see EVERY successful deletion at INFO so a silent
     * crash-recovery cleanup never goes unnoticed — silent recovery
     * is exactly the kind of thing that bites once an unrelated bug
     * leaks `.tmp` files faster than they're cleaned. Per-file IO
     * failures degrade to WARN so one unreadable entry never blocks
     * the rest of the sweep.
     *
     * Idempotent — a second invocation finds nothing and is a no-op.
     * Only `.tmp` entries are touched; canonical `.json` files and
     * anything else in the directory are left alone.
     */
    private fun sweepOrphans() {
        val stream = try {
            Files.newDirectoryStream(directory, "*$TMP_SUFFIX")
        } catch (e: NoSuchFileException) {
            // The directory was racing with us (deleted between
            // `createDirectories` above and here, or otherwise gone).
            // Mirrors the `save` idiom of treating the directory as
            // lazy: nothing to sweep, nothing to log loudly about.
            logger.debug(
                "FileCheckpointStore: sweep directory {} missing; nothing to do.",
                directory, e,
            )
            return
        } catch (e: IOException) {
            logger.warn(
                "FileCheckpointStore: could not list {} for orphan sweep ({}); skipping cleanup.",
                directory, e.javaClass.simpleName, e,
            )
            return
        }
        stream.use { entries ->
            for (orphan in entries) {
                try {
                    Files.deleteIfExists(orphan)
                    logger.info("FileCheckpointStore: swept orphan checkpoint temp file {}", orphan)
                    metrics.recordCheckpointOrphanSwept(SWEEP_OUTCOME_DELETED)
                } catch (e: IOException) {
                    // One unreadable file must not abort the rest of
                    // the sweep — log and move on. The metric carries
                    // the failure signal even if the log is muted.
                    logger.warn(
                        "FileCheckpointStore: failed to delete orphan {} ({}); leaving it in place.",
                        orphan, e.javaClass.simpleName, e,
                    )
                    metrics.recordCheckpointOrphanSwept(SWEEP_OUTCOME_FAILED)
                }
            }
        }
    }

    // ReturnCount: three early-exit branches (file missing, unreadable,
    // decoded) are clearer than a nested if/else; each is a distinct
    // outcome the caller has to handle.
    @Suppress("ReturnCount")
    override fun load(key: String): String? {
        val file = pathFor(key)
        val raw = try {
            Files.readString(file, StandardCharsets.UTF_8)
        } catch (e: NoSuchFileException) {
            logger.debug(
                "FileCheckpointStore: checkpoint file {} not present; treating as missing.",
                file, e,
            )
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
                directory, e,
            )
            Files.move(tmp, canonical, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private fun pathFor(key: String): Path = directory.resolve("${sanitise(key)}$JSON_SUFFIX")

    private fun encode(key: String, value: String): String =
        """{"key":"${escape(key)}","value":"${escape(value)}"}"""

    // ReturnCount: parser preflight is naturally a sequence of guards
    // (shape, required field, key match) — flattening to nested ifs
    // would make each invariant harder to read.
    @Suppress("ReturnCount")
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
    // ReturnCount: a small hand-rolled JSON-string scanner naturally
    // has multiple bail-outs (not found, closing quote, EOF) — each
    // is a distinct case the caller does not need to disambiguate.
    @Suppress("ReturnCount")
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
        private const val JSON_SUFFIX = ".json"
        private const val TMP_SUFFIX = ".tmp"
        private const val SWEEP_OUTCOME_DELETED = "deleted"
        private const val SWEEP_OUTCOME_FAILED = "failed"
    }
}
