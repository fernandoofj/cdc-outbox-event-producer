package br.com.fltech.outbox.publisher.adapter.dlq.replay

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Read-only DTO for the dead-letter envelope produced by
 * `SqsDeadLetterSink`. Mirrors that writer's `linkedMapOf` shape
 * field-for-field so DLQs already accumulated in production are
 * replayable without any migration.
 *
 * `@JsonIgnoreProperties(ignoreUnknown = true)` ensures forward
 * compatibility if the writer adds new fields — the replay reader
 * keeps working against the older shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class DlqEnvelope(
    /** Routing prefix as emitted by the source (e.g. `sns://orders-events`). */
    @JsonProperty("originalPrefix")
    val originalPrefix: String,

    /** Canonical Postgres LSN string `X/X` (`asString()` form). */
    @JsonProperty("lsn")
    val lsn: String,

    /** Original payload — kept as a String so a parse error on the
     * way in does not block the replay tool from reading it back. */
    @JsonProperty("content")
    val content: String,

    /** Simple class name of the last exception that caused the dead-letter. */
    @JsonProperty("failureType")
    val failureType: String,

    /** Exception message; empty string when the exception had none. */
    @JsonProperty("failureMessage")
    val failureMessage: String,

    /** ISO-8601 UTC timestamp of when the message was dead-lettered. */
    @JsonProperty("deadLetteredAt")
    val deadLetteredAt: String,
)
