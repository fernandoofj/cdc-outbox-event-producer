package br.com.fltech.cdc.outbox.publisher.retry

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ExponentialBackOffTest {

    @Test
    fun `no-jitter policy doubles each attempt and caps at max`() {
        val policy = ExponentialBackOff(
            initial = Duration.ofMillis(100),
            max = Duration.ofMillis(800),
            multiplier = 2.0,
            jitter = 0.0,
        )

        assertEquals(100L, policy.nextDelay(1).toMillis(), "attempt 1: initial")
        assertEquals(200L, policy.nextDelay(2).toMillis(), "attempt 2: doubled")
        assertEquals(400L, policy.nextDelay(3).toMillis(), "attempt 3: doubled again")
        assertEquals(800L, policy.nextDelay(4).toMillis(), "attempt 4: capped at max")
        assertEquals(800L, policy.nextDelay(50).toMillis(), "many attempts: still capped")
    }

    @Test
    fun `attempt less than 1 is treated as attempt 1`() {
        val policy = ExponentialBackOff(
            initial = Duration.ofMillis(500),
            max = Duration.ofSeconds(10),
            multiplier = 2.0,
            jitter = 0.0,
        )
        assertEquals(500L, policy.nextDelay(0).toMillis())
        assertEquals(500L, policy.nextDelay(-5).toMillis())
    }

    @Test
    fun `jitter keeps each delay within plus or minus the jitter factor of the base`() {
        val policy = ExponentialBackOff(
            initial = Duration.ofMillis(1000),
            max = Duration.ofSeconds(10),
            multiplier = 2.0,
            jitter = 0.3,
            random = Random(seed = 42L),
        )

        repeat(BIG_SAMPLE) {
            val attempt = (it % 5) + 1
            val expectedBaseMs = minOf(1000L * (1L shl (attempt - 1)), 10_000L)
            val actual = policy.nextDelay(attempt).toMillis()
            val lower = (expectedBaseMs * (1.0 - 0.3)).toLong()
            val upper = (expectedBaseMs * (1.0 + 0.3)).toLong()
            assertTrue(
                actual in lower..upper,
                "attempt=$attempt: $actual not in [$lower, $upper]",
            )
        }
    }

    @Test
    fun `invalid configuration is rejected at construction`() {
        assertThrows<IllegalArgumentException> {
            ExponentialBackOff(initial = Duration.ZERO)
        }
        assertThrows<IllegalArgumentException> {
            ExponentialBackOff(initial = Duration.ofSeconds(5), max = Duration.ofSeconds(1))
        }
        assertThrows<IllegalArgumentException> {
            ExponentialBackOff(multiplier = 1.0)
        }
        assertThrows<IllegalArgumentException> {
            ExponentialBackOff(jitter = 1.5)
        }
        assertThrows<IllegalArgumentException> {
            ExponentialBackOff(jitter = -0.1)
        }
    }

    companion object {
        private const val BIG_SAMPLE = 200
    }
}
