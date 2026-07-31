package br.com.fltech.outbox.publisher.adapter.dlq.replay

import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation
import org.springframework.boot.actuate.endpoint.annotation.Endpoint
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation
import org.springframework.boot.actuate.endpoint.annotation.Selector
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

/**
 * Spring Boot Actuator endpoint exposing DLQ replay operations at
 * `/actuator/cdcOutboxDlq`. Each method asserts that the request
 * carries an authenticated `SecurityContext` before doing any work
 * — defence-in-depth on top of whatever `SecurityFilterChain` the
 * consumer's app configures.
 *
 * The wider auto-configuration ([CdcOutboxDlqReplayAutoConfiguration]
 * in the spring-boot-starter) gates the bean registration on
 * `org.springframework.security.web.SecurityFilterChain` being on
 * the classpath, so this endpoint can never run in an environment
 * where Spring Security is absent. The runtime auth check below
 * catches the secondary case — a present-but-permissive
 * `SecurityFilterChain` that allowed an anonymous request through.
 */
@Endpoint(id = ENDPOINT_ID)
class DlqReplayActuatorEndpoint(
    private val service: DlqReplayService,
) {
    @ReadOperation
    fun listMessages(
        @Selector(match = Selector.Match.SINGLE) action: String?,
    ): Any {
        requireAuthenticated()
        return when (action) {
            null, "messages" -> mapOf("messages" to service.peek(DEFAULT_PEEK_SIZE))
            "stats" -> service.stats()
            else -> mapOf("error" to "unknown action '$action'; valid: messages, stats")
        }
    }

    @WriteOperation
    fun replay(
        @Selector(match = Selector.Match.SINGLE) action: String,
        body: ReplayRequest?,
    ): Any {
        requireAuthenticated()
        return when (action) {
            "replay" -> {
                val req = body ?: return mapOf("error" to "missing request body for replay")
                val override = req.toOverride()
                service.replay(req.handle, req.envelope, override)
            }
            "replay-bulk" -> {
                val max = body?.bulkMax ?: DEFAULT_BULK_SIZE
                val dryRun = body?.dryRun ?: false
                service.replayBulk(max, dryRun)
            }
            else -> mapOf("error" to "unknown action '$action'; valid: replay, replay-bulk")
        }
    }

    @DeleteOperation
    fun abandon(
        @Selector(match = Selector.Match.SINGLE) handle: String,
    ): Any {
        requireAuthenticated()
        return service.abandon(handle)
    }

    private fun requireAuthenticated() {
        val authentication = SecurityContextHolder.getContext().authentication
        if (authentication == null || !authentication.isAuthenticated || authentication.isAnonymous()) {
            throw AccessDeniedException(
                "cdc-outbox DLQ replay endpoint requires an authenticated principal. " +
                    "Configure a SecurityFilterChain that requires authentication on /actuator/cdcOutboxDlq.",
            )
        }
    }

    private fun org.springframework.security.core.Authentication.isAnonymous(): Boolean =
        authorities?.any { it.authority == "ROLE_ANONYMOUS" } == true ||
            this is AnonymousAuthenticationToken

    /**
     * Request body for both single and bulk replay paths — fields
     * are nullable so callers can post a minimal JSON for the bulk
     * mode (`{"bulkMax": 20, "dryRun": true}`) or a full envelope
     * for single replay.
     */
    data class ReplayRequest(
        val handle: String = "",
        val envelope: DlqEnvelope = EMPTY_ENVELOPE,
        val overrideScheme: String? = null,
        val overrideTarget: String? = null,
        val bulkMax: Int? = null,
        val dryRun: Boolean? = null,
    ) {
        fun toOverride(): DlqReplayService.RoutingOverride? {
            val scheme = overrideScheme?.takeIf { it.isNotBlank() }
            val target = overrideTarget?.takeIf { it.isNotBlank() }
            return if (scheme != null && target != null) {
                DlqReplayService.RoutingOverride(scheme, target)
            } else {
                null
            }
        }

        companion object {
            private val EMPTY_ENVELOPE = DlqEnvelope("", "", "", "", "", "")
        }
    }

    companion object {
        private const val DEFAULT_PEEK_SIZE = 10
        private const val DEFAULT_BULK_SIZE = 10
    }
}

private const val ENDPOINT_ID = "cdcOutboxDlq"
