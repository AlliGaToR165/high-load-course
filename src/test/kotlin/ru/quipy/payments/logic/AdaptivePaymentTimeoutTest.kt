package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration

class AdaptivePaymentTimeoutTest {
    @Test
    fun `cold start scales with each account and state is isolated`() {
        val fast = AdaptivePaymentTimeout(Duration.ofMillis(100), AdaptiveTimeoutSettings())
        val slow = AdaptivePaymentTimeout(Duration.ofSeconds(3), AdaptiveTimeoutSettings())
        assertEquals(125, fast.timeout().toMillis())
        assertEquals(3750, slow.timeout().toMillis())
        repeat(100) { fast.observe(500, false) }
        assertTrue(fast.timeout().toMillis() > 500)
        assertEquals(3750, slow.timeout().toMillis())
        assertEquals(0, slow.sampleCount())
    }

    @Test
    fun `censored calls are not treated as completed at the timeout`() {
        val estimator = AdaptivePaymentTimeout(Duration.ofMillis(100),
            AdaptiveTimeoutSettings(windowSize = 10, minSamples = 10, smoothing = 1.0))
        repeat(8) { estimator.observe(100, true) }
        repeat(2) { estimator.observe(1000, false) }
        assertEquals(1250, estimator.timeout().toMillis())
        assertEquals(0.8, estimator.censoredFraction(), 0.001)
    }

    @Test
    fun `occasional long tail does not force the budget to follow outliers`() {
        val estimator = AdaptivePaymentTimeout(Duration.ofMillis(1200), AdaptiveTimeoutSettings())
        repeat(1000) { i ->
            val latency = if (i % 10 == 0) 9500L else (i % 10) * 100L
            observeAttempt(estimator, latency)
        }
        assertTrue(estimator.timeout().toMillis() in 900..1500)
        assertEquals(256, estimator.sampleCount())
    }

    @Test
    fun `timeouts reveal a slowdown and old slow samples eventually expire`() {
        val estimator = AdaptivePaymentTimeout(Duration.ofMillis(100), AdaptiveTimeoutSettings())
        repeat(300) { observeAttempt(estimator, 100) }
        assertTrue(estimator.timeout().toMillis() in 100..150)
        repeat(600) { observeAttempt(estimator, 3000) }
        assertTrue(estimator.timeout().toMillis() >= 3000)
        assertEquals(0.0, estimator.censoredFraction())
        repeat(600) { observeAttempt(estimator, 100) }
        assertTrue(estimator.timeout().toMillis() in 100..150)
    }

    @Test
    fun `deadline capped timeouts cannot make the estimate grow without evidence`() {
        val estimator = AdaptivePaymentTimeout(Duration.ofMillis(100), AdaptiveTimeoutSettings())
        repeat(1000) { estimator.observe(500, true) }
        assertTrue(estimator.timeout().toMillis() in 500..626)
        assertEquals(1.0, estimator.censoredFraction())
    }

    @Test
    fun `invalid policy is rejected`() {
        assertThrows(IllegalArgumentException::class.java) { AdaptiveTimeoutSettings(quantile = 1.0) }
        assertThrows(IllegalArgumentException::class.java) { AdaptiveTimeoutSettings(windowSize = 10, minSamples = 20) }
        assertThrows(IllegalArgumentException::class.java) { AdaptiveTimeoutSettings(headroom = Double.NaN) }
        assertThrows(IllegalArgumentException::class.java) { AdaptiveTimeoutSettings(smoothing = 0.0) }
        assertThrows(IllegalArgumentException::class.java) {
            AdaptivePaymentTimeout(Duration.ZERO, AdaptiveTimeoutSettings())
        }
    }

    private fun observeAttempt(estimator: AdaptivePaymentTimeout, actualMillis: Long) {
        val budget = estimator.timeout().toMillis()
        estimator.observe(minOf(actualMillis, budget), actualMillis > budget)
    }
}
