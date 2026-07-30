package br.com.fltech.outbox.publisher.core.port

/**
 * Reports CDC replication lag in bytes — how far the consumer's
 * checkpoint trails the upstream head of the WAL (Postgres) or the
 * binary log (MySQL).
 *
 * Surfaced as a Micrometer gauge by the observability layer so
 * operators can SLO-alert on a backlog growing faster than the
 * publisher drains it. The metric complements `messages.read` and
 * `publish.duration`: read-rate alone cannot distinguish "idle source"
 * from "publisher falling behind a busy source", but lag-bytes can.
 *
 * Implementations are expected to be cheap (one round trip to the
 * upstream) because [lagBytes] is invoked from the scheduler on a
 * configurable interval. Heavy work (e.g. binlog scans) MUST be
 * avoided here — return `null` rather than block. Returning `null`
 * leaves the gauge un-updated, which Micrometer represents as "no
 * data" and Prometheus carries forward as a stale sample for one
 * scrape; alerting rules should already tolerate transient gaps.
 *
 * Lifecycle: the probe is a passive query. Its dependencies
 * (DataSource, CheckpointStore) own their own connect/close
 * semantics. The probe itself has nothing to open or close.
 */
interface LagProbe {
    /**
     * Symbolic source label used as the `source` tag on the
     * Micrometer gauge. Must be stable across restarts so dashboards
     * don't drift. Typical values: `"postgres"`, `"mysql"`.
     */
    val sourceLabel: String

    /**
     * Returns the current lag in bytes, or `null` when the
     * measurement is temporarily unavailable (e.g. SQL exception,
     * binlog file rotated mid-window, no checkpoint persisted yet).
     *
     * Returning `null` is the correct conservative answer when the
     * probe genuinely cannot compute a value — emit a WARN log at the
     * call site so the gap is visible without taking the scheduler
     * loop down. Implementations MUST NOT silently swallow errors.
     */
    fun lagBytes(): Long?
}
