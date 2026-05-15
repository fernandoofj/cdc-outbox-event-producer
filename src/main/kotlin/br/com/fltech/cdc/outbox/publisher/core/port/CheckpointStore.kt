package br.com.fltech.cdc.outbox.publisher.core.port

/**
 * Port for persisting per-source CDC checkpoint markers (binlog
 * filename/position, Postgres LSN, etc.) across process restarts.
 *
 * Implementations MUST be atomic on `save` — a partial write left
 * behind by a crash mid-`save` is worse than no checkpoint at all,
 * because it would silently resume from an undefined position. The
 * default file-backed implementation
 * ([br.com.fltech.cdc.outbox.publisher.adapter.checkpoint.FileCheckpointStore])
 * achieves atomicity by writing to a sibling `.tmp` file, `fsync`ing
 * it, and renaming over the target.
 *
 * The `key` argument namespaces unrelated sources (e.g. one MySQL
 * binlog and one Postgres slot in the same process) so a single store
 * can back multiple [br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource]
 * instances. Adapters are responsible for picking a stable, unique
 * key — typically `"<source-kind>:<server-or-slot-id>"`.
 *
 * `save` is called per-ack on the orchestrator's single thread. The
 * port is intentionally non-blocking from a contract perspective:
 * implementations are expected to keep the I/O cheap enough that the
 * per-event overhead is dominated by the broker publish.
 *
 * Lifecycle: implementations that hold OS resources (file handles,
 * JDBC connections) should release them on [close]. The default
 * implementation has nothing to close; the `Unit` default is
 * convenience for that case.
 */
interface CheckpointStore : AutoCloseable {

    /**
     * Return the last value persisted under [key], or `null` if no
     * value was stored — or if the persisted value was found to be
     * corrupt and the implementation chose to treat that as missing
     * (WARN-logging the corruption rather than failing the source's
     * `open()`). The "treat corrupt as missing" branch is preferred
     * because it lets the source fall back to its natural start
     * position (head of binlog, current LSN) instead of refusing to
     * start.
     */
    fun load(key: String): String?

    /**
     * Persist [value] under [key]. Atomic — a crash during `save`
     * leaves either the previous value or no value, never a
     * partial write. Idempotent — calling `save` with the same
     * `(key, value)` pair twice is a no-op from a correctness
     * standpoint.
     */
    fun save(key: String, value: String)

    override fun close() = Unit
}
