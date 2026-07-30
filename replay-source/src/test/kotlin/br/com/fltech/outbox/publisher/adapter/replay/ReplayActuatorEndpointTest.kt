package br.com.fltech.outbox.publisher.adapter.replay

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.RowChange
import br.com.fltech.outbox.publisher.core.port.EventSink
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.core.port.MappingRules
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * Covers `requireAuthenticated()` against the real Spring Security 7.0.5
 * types — added after the Boot 4 migration (Security 6.3.4 -> 7.0.5) left
 * this endpoint's only auth gate with zero test coverage.
 */
class ReplayActuatorEndpointTest {

    private val endpoint = ReplayActuatorEndpoint(emptyReplayService())

    @AfterTest
    fun clearContext() {
        SecurityContextHolder.clearContext()
    }

    @Test
    fun `null authentication is rejected`() {
        SecurityContextHolder.clearContext()

        assertFailsWith<AccessDeniedException> { endpoint.snapshot() }
    }

    @Test
    fun `unauthenticated principal is rejected`() {
        SecurityContextHolder.getContext().authentication =
            UsernamePasswordAuthenticationToken.unauthenticated("user", "pw")

        assertFailsWith<AccessDeniedException> { endpoint.jobById("j1") }
    }

    @Test
    fun `real AnonymousAuthenticationToken is rejected even when marked authenticated`() {
        SecurityContextHolder.getContext().authentication = AnonymousAuthenticationToken(
            "key",
            "anonymousUser",
            listOf(SimpleGrantedAuthority("ROLE_ANONYMOUS")),
        )

        assertFailsWith<AccessDeniedException> { endpoint.snapshot() }
    }

    @Test
    fun `AnonymousAuthenticationToken is rejected even with a non-standard authority`() {
        // Isolates the isAnonymous() class-name fallback: an app that
        // configures AnonymousAuthenticationFilter with a custom authority
        // (not ROLE_ANONYMOUS) must still be caught by the class check, not
        // just the authority check the other anonymous test also satisfies.
        SecurityContextHolder.getContext().authentication = AnonymousAuthenticationToken(
            "key",
            "anonymousUser",
            listOf(SimpleGrantedAuthority("ROLE_GUEST")),
        )

        assertFailsWith<AccessDeniedException> { endpoint.snapshot() }
    }

    @Test
    fun `authenticated non-anonymous principal is let through`() {
        SecurityContextHolder.getContext().authentication = UsernamePasswordAuthenticationToken.authenticated(
            "operator",
            null,
            listOf(SimpleGrantedAuthority("ROLE_OPERATOR")),
        )

        val result = endpoint.snapshot() as Map<*, *>

        assertEquals(emptyList<Any>(), result["finished"])
    }

    private fun emptyReplayService(): ReplayService {
        val meterRegistry = SimpleMeterRegistry()
        val metrics = CdcOutboxMetrics(meterRegistry)
        val registry = object : EventSinkRegistry {
            override fun publish(routing: br.com.fltech.outbox.publisher.core.domain.Routing, event: OutboxEvent) = Unit
            override fun knownSchemes(): Set<String> = emptySet()
            override fun resolve(scheme: String): EventSink? = null
        }
        val rules = object : MappingRules {
            override fun map(rowChange: RowChange): OutboxEvent? = null
        }
        return ReplayService(replayers = emptyMap(), mappingRules = rules, sinkRegistry = registry, metrics = metrics)
    }
}
