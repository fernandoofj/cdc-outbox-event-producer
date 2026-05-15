package br.com.fltech.cdc.outbox.publisher.retry

import java.time.Duration
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

/**
 * Pluggable retry-delay policy.
 *
 * Implementations must be pure: given the same [attempt] and the same internal
 * state, they should return reproducibly bounded delays. Randomness MUST be
 * injectable so tests can deterministically pin jitter.
 */
interface BackOff {
    /**
     * Returns the delay to apply BEFORE attempt number [attempt] (1-based).
     * The returned [Duration] must be non-negative.
     */
    fun nextDelay(attempt: Int): Duration
}

/**
 * Exponential back-off with full jitter, capped at [max].
 *
 * Delay grows as `initial * multiplier^(attempt-1)`, capped at [max], then
 * scaled by a uniform random factor in `[1 - jitter, 1 + jitter]`. With
 * [jitter] = 0.3, two retriers running in lockstep desynchronise within a
 * handful of attempts; with [jitter] = 0.0 the policy is deterministic
 * (useful for tests).
 */
data class ExponentialBackOff(
    val initial: Duration = Duration.ofMillis(DEFAULT_INITIAL_MS),
    val max: Duration = Duration.ofSeconds(DEFAULT_MAX_SECONDS),
    val multiplier: Double = DEFAULT_MULTIPLIER,
    val jitter: Double = DEFAULT_JITTER,
    private val random: Random = Random.Default,
) : BackOff {

    init {
        require(!initial.isNegative && !initial.isZero) { "initial must be > 0" }
        require(max >= initial) { "max ($max) must be >= initial ($initial)" }
        require(multiplier > 1.0) { "multiplier ($multiplier) must be > 1.0" }
        require(jitter in 0.0..1.0) { "jitter ($jitter) must be in [0.0, 1.0]" }
    }

    override fun nextDelay(attempt: Int): Duration {
        val safeAttempt = attempt.coerceAtLeast(1)
        val exponent = (safeAttempt - 1).toDouble()
        val raw = initial.toMillis() * multiplier.pow(exponent)
        val capped = min(raw, max.toMillis().toDouble())
        val jitterFactor = if (jitter == 0.0) {
            1.0
        } else {
            1.0 + jitter * (random.nextDouble() * 2.0 - 1.0)
        }
        val finalMs = (capped * jitterFactor).coerceAtLeast(0.0).toLong()
        return Duration.ofMillis(finalMs)
    }

    companion object {
        const val DEFAULT_INITIAL_MS = 200L
        const val DEFAULT_MAX_SECONDS = 30L
        const val DEFAULT_MULTIPLIER = 2.0
        const val DEFAULT_JITTER = 0.3
    }
}
