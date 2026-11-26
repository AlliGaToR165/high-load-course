package ru.quipy.payments.logic

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.util.ArrayDeque
import kotlin.math.ceil

@ConfigurationProperties(prefix = "payment.retry.adaptive")
data class AdaptiveTimeoutSettings(
    val enabled: Boolean = true,
    val windowSize: Int = 256,
    val minSamples: Int = 32,
    val quantile: Double = 0.8,
    val headroom: Double = 1.25,
    val smoothing: Double = 0.1,
) {
    init {
        require(windowSize > 0 && minSamples in 1..windowSize)
        require(quantile.isFinite() && quantile > 0 && quantile < 1)
        require(headroom.isFinite() && headroom > 1)
        require(smoothing.isFinite() && smoothing > 0 && smoothing <= 1)
    }
}

class AdaptivePaymentTimeout(
    initialProcessingTime: Duration,
    private val settings: AdaptiveTimeoutSettings,
) {
    private data class Observation(val millis: Long, val censored: Boolean)
    private val observations = ArrayDeque<Observation>()
    private val initialMillis = initialProcessingTime.toMillis().also { require(it > 0) }.toDouble()
    private var estimateMillis = initialMillis * settings.headroom

    @Synchronized
    fun timeout(): Duration = Duration.ofMillis(ceil(estimateMillis).toLong().coerceAtLeast(1))

    @Synchronized
    fun sampleCount(): Int = observations.size

    @Synchronized
    fun censoredFraction(): Double = if (observations.isEmpty()) 0.0 else
        observations.count { it.censored }.toDouble() / observations.size

    // A timed-out call is only a lower bound on its completion time.
    @Synchronized
    fun observe(millis: Long, censored: Boolean) {
        require(millis > 0)
        observations.addLast(Observation(millis, censored))
        if (observations.size > settings.windowSize) observations.removeFirst()
        if (observations.size < settings.minSamples) return

        // Kaplan-Meier estimate: remove censored observations from the risk set,
        // but do not count them as completed requests. Completions precede censoring at ties.
        val sorted = observations.sortedBy { it.millis }
        var atRisk = sorted.size
        var survival = 1.0
        var index = 0
        var quantileMillis: Long? = null
        while (index < sorted.size) {
            val millisAtTime = sorted[index].millis
            var end = index
            var completed = 0
            while (end < sorted.size && sorted[end].millis == millisAtTime) {
                if (!sorted[end].censored) completed++
                end++
            }
            survival *= 1.0 - completed.toDouble() / atRisk
            if (survival <= 1.0 - settings.quantile + 1e-12) {
                quantileMillis = millisAtTime
                break
            }
            atRisk -= end - index
            index = end
        }
        // If the quantile is hidden by timeouts, increase the budget beyond observed
        // lower bounds. The caller still clips every attempt to its payment deadline.
        val target = if (quantileMillis != null) quantileMillis.toDouble() * settings.headroom else
            maxOf(initialMillis, sorted.last().millis.toDouble()) * settings.headroom
        estimateMillis += settings.smoothing * (target - estimateMillis)
    }
}
