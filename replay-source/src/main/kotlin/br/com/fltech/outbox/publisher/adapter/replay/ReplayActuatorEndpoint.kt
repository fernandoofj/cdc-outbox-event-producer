package br.com.fltech.outbox.publisher.adapter.replay

import br.com.fltech.outbox.publisher.core.port.UnsupportedReplayException
import org.springframework.boot.actuate.endpoint.annotation.Endpoint
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation
import org.springframework.boot.actuate.endpoint.annotation.Selector
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.core.context.SecurityContextHolder

/**
 * Operator-facing Actuator endpoint at `/actuator/cdcOutboxReplay`
 * for source-side replay jobs.
 *
 * Auth model — identical to the DLQ replay endpoint:
 *  - The wider auto-config is gated on Spring Security being on
 *    the classpath. If absent, this endpoint is not wired.
 *  - Every operation method calls [requireAuthenticated] which
 *    inspects the current `SecurityContext`. Anonymous requests
 *    are refused even if the consumer's `SecurityFilterChain`
 *    was misconfigured to permit them.
 *
 * Operations:
 *  - `POST /actuator/cdcOutboxReplay/start` — launches a replay
 *    job, returns `{jobId, status: RUNNING}` immediately.
 *  - `GET /actuator/cdcOutboxReplay/{jobId}` — returns the
 *    snapshot of the named job (running or finished).
 *  - `GET /actuator/cdcOutboxReplay` — returns the currently
 *    running job (or `null`) plus a list of recently-finished
 *    jobs.
 */
@Endpoint(id = ENDPOINT_ID)
class ReplayActuatorEndpoint(
    private val service: ReplayService,
) {

    @ReadOperation
    fun snapshot(): Any {
        requireAuthenticated()
        return mapOf(
            "finished" to service.finishedJobs(),
        )
    }

    @ReadOperation
    fun jobById(@Selector(match = Selector.Match.SINGLE) jobId: String): Any {
        requireAuthenticated()
        return service.getJob(jobId) ?: mapOf("error" to "no job with id='$jobId'")
    }

    @WriteOperation
    fun start(@Selector(match = Selector.Match.SINGLE) action: String, body: ReplayRequest): Any {
        requireAuthenticated()
        return when (action) {
            "start" -> tryStart(body)
            else -> mapOf("error" to "unknown action '$action'; valid: start")
        }
    }

    private fun tryStart(body: ReplayRequest): Any = try {
        service.startReplay(body)
    } catch (e: ConcurrentReplayException) {
        mapOf(
            "error" to "another replay is already running",
            "detail" to e.message,
        )
    } catch (e: UnsupportedReplayException) {
        mapOf(
            "error" to "replay not supported for the requested source",
            "detail" to e.message,
        )
    }

    private fun requireAuthenticated() {
        val authentication = SecurityContextHolder.getContext().authentication
        if (authentication == null || !authentication.isAuthenticated || authentication.isAnonymous()) {
            throw AccessDeniedException(
                "cdc-outbox replay endpoint requires an authenticated principal. " +
                    "Configure a SecurityFilterChain that requires authentication on " +
                    "/actuator/cdcOutboxReplay.",
            )
        }
    }

    private fun org.springframework.security.core.Authentication.isAnonymous(): Boolean =
        authorities?.any { it.authority == "ROLE_ANONYMOUS" } == true ||
            javaClass.simpleName == "AnonymousAuthenticationToken"
}

private const val ENDPOINT_ID = "cdcOutboxReplay"
