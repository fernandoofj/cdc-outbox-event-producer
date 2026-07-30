package br.com.fltech.outbox.publisher.core.port

/**
 * Driving port for replaying a bounded window of historic source
 * events. Implementations open a one-shot stream over the source
 * (MySQL binlog or Postgres WAL) that starts at [fromPosition] and
 * ends once [toPosition] is reached or the source is exhausted.
 *
 * The returned [RowChangeSource] follows the normal contract:
 * `open()` → `poll()` loop → `ack()` (no-op in replay — the live
 * processor's checkpoint state is never touched) → `close()`.
 * `poll()` returns `null` once the window has been fully drained.
 *
 * Each port instance declares a [sourceKind] string — `"mysql-binlog"`,
 * `"postgres-wal"` — that the operator-facing endpoint uses to
 * dispatch a replay request to the right adapter.
 */
interface SourceReplayer {

    /** Symbolic source label used by the dispatcher to pick the right adapter. */
    val sourceKind: String

    /**
     * Returns a bounded [RowChangeSource] that drains the requested
     * window. The position strings are adapter-specific (MySQL uses
     * `"<binlog-file>:<position>"`; Postgres uses canonical
     * `"X/X"` LSNs).
     *
     * Implementations MUST throw [UnsupportedReplayException] when
     * the request cannot be honoured (e.g., the Postgres stub) so
     * the dispatcher fails fast rather than silently emitting nothing.
     */
    fun openBoundedSource(fromPosition: String, toPosition: String): RowChangeSource
}

/**
 * Thrown by a [SourceReplayer] that does not (yet) support the
 * requested window. Carries the human-readable reason so the
 * operator endpoint can surface it directly.
 */
class UnsupportedReplayException(message: String) : RuntimeException(message)
