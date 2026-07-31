package br.com.fltech.outbox.publisher.adapter.dlq.replay

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

/**
 * Covers `requireAuthenticated()` against the real Spring Security 7.0.5
 * types — added after the Boot 4 migration (Security 6.3.4 -> 7.0.5) left
 * this endpoint's only auth gate with zero test coverage.
 */
class DlqReplayActuatorEndpointTest {
    private val service = mockk<DlqReplayService>(relaxed = true)
    private val endpoint = DlqReplayActuatorEndpoint(service)

    @AfterTest
    fun clearContext() {
        SecurityContextHolder.clearContext()
    }

    @Test
    fun `null authentication is rejected`() {
        SecurityContextHolder.clearContext()

        assertFailsWith<AccessDeniedException> { endpoint.listMessages(null) }
        verify(exactly = 0) { service.peek(any()) }
    }

    @Test
    fun `unauthenticated principal is rejected`() {
        val auth = UsernamePasswordAuthenticationToken.unauthenticated("user", "pw")
        SecurityContextHolder.getContext().authentication = auth

        assertFailsWith<AccessDeniedException> { endpoint.listMessages(null) }
        verify(exactly = 0) { service.peek(any()) }
    }

    @Test
    fun `real AnonymousAuthenticationToken is rejected even when marked authenticated`() {
        val anonymous =
            AnonymousAuthenticationToken(
                "key",
                "anonymousUser",
                listOf(SimpleGrantedAuthority("ROLE_ANONYMOUS")),
            )
        SecurityContextHolder.getContext().authentication = anonymous

        assertFailsWith<AccessDeniedException> { endpoint.abandon("h1") }
        verify(exactly = 0) { service.abandon(any()) }
    }

    @Test
    fun `AnonymousAuthenticationToken is rejected even with a non-standard authority`() {
        // Isolates the isAnonymous() type-check fallback: an app that
        // configures AnonymousAuthenticationFilter with a custom authority
        // (not ROLE_ANONYMOUS) must still be caught by the type check, not
        // just the authority check the other anonymous test also satisfies.
        val anonymous =
            AnonymousAuthenticationToken(
                "key",
                "anonymousUser",
                listOf(SimpleGrantedAuthority("ROLE_GUEST")),
            )
        SecurityContextHolder.getContext().authentication = anonymous

        assertFailsWith<AccessDeniedException> { endpoint.abandon("h1") }
        verify(exactly = 0) { service.abandon(any()) }
    }

    @Test
    fun `authenticated non-anonymous principal is let through`() {
        val auth =
            UsernamePasswordAuthenticationToken.authenticated(
                "operator",
                null,
                listOf(SimpleGrantedAuthority("ROLE_OPERATOR")),
            )
        SecurityContextHolder.getContext().authentication = auth
        every { service.peek(any()) } returns emptyList()

        endpoint.listMessages(null)

        verify(exactly = 1) { service.peek(any()) }
    }
}
