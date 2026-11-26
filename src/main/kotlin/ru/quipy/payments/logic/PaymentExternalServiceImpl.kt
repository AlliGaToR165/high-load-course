package ru.quipy.payments.logic

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import ru.quipy.apigateway.ratelimit.PaymentCallAdmission
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.ceil


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentCallAdmission: PaymentCallAdmission,
    private val maxAttempts: Int,
    private val retryableStatuses: Set<Int>,
    processingTimeFactor: Double,
    private val attemptTimeout: Duration = Duration.ZERO,
    private val responseTimeoutReserve: Duration = Duration.ofMillis(300),
    adaptiveSettings: AdaptiveTimeoutSettings = AdaptiveTimeoutSettings(),
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    init {
        require(!attemptTimeout.isNegative) { "Attempt timeout must not be negative" }
        require(responseTimeoutReserve.toMillis() > 0) { "Response timeout reserve must be positive" }
        require(attemptTimeout.isZero || attemptTimeout.toMillis() > 0) { "Attempt timeout must be at least one millisecond" }
    }

    private val adaptiveTimeout = if (adaptiveSettings.enabled && attemptTimeout.isZero)
        AdaptivePaymentTimeout(properties.averageProcessingTime, adaptiveSettings) else null
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val retryProcessingTime = Duration.ofMillis(ceil(requestAverageProcessingTime.toMillis() * processingTimeFactor).toLong())
    private val endpoint = "http://$paymentProviderHostPort/external/process".toHttpUrl()
    private val metricTags = Tags.of("account", accountName)
    private val deadlineDropped = Metrics.counter("payment_deadline_dropped", metricTags)
    private val retryCounters = (retryableStatuses.map { "http_$it" } + "temporary_error").associateWith { reason ->
        Metrics.counter("payment_retries", metricTags.and("reason", reason))
    }

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .followRedirects(false)
        .addNetworkInterceptor { chain ->
            val response = chain.proceed(chain.request())
            if (response.code == HttpStatus.SERVICE_UNAVAILABLE.value() && retryAfterMillis(response.header("Retry-After")) == 0L) {
                response.use { throw RetryableResponse(readOutcome(it)) }
            }
            response
        }
        .build()

    init {
        logger.info("[$accountName] Payment retry configuration: adaptive=${adaptiveTimeout != null}, attemptTimeout=${adaptiveTimeout?.timeout() ?: attemptTimeout}, responseTimeoutReserve=$responseTimeoutReserve, maxAttempts=$maxAttempts, retryableStatuses=$retryableStatuses")
        adaptiveTimeout?.let { estimator ->
            Metrics.gauge("payment_adaptive_timeout_seconds", metricTags, estimator) { it.timeout().toMillis() / 1000.0 }
            Metrics.gauge("payment_adaptive_samples", metricTags, estimator) { it.sampleCount().toDouble() }
            Metrics.gauge("payment_adaptive_censored_fraction", metricTags, estimator) { it.censoredFraction() }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var retryDelayMs = 0L
        var retryReason: String? = null
        repeat(maxAttempts) { attempt ->
            val providerAttemptTimeout = adaptiveTimeout?.timeout() ?: attemptTimeout
            val attemptBudget = if (providerAttemptTimeout.isZero) null else providerAttemptTimeout.plus(responseTimeoutReserve)
            val processingTimeEstimate = attemptBudget ?: if (attempt == 0) requestAverageProcessingTime else retryProcessingTime
            val outcome = performAttempt(paymentId, amount, paymentStartedAt, deadline, retryDelayMs, processingTimeEstimate, retryReason, providerAttemptTimeout)
            if (outcome.success || !outcome.retryable) return
            retryDelayMs = outcome.retryDelayMs
            retryReason = outcome.retryReason
        }
    }

    private fun performAttempt(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, retryDelayMs: Long, processingTimeEstimate: Duration, retryReason: String?, providerAttemptTimeout: Duration): Outcome {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val admissionEstimate = if (adaptiveTimeout == null) processingTimeEstimate else {
            val availableMs = remainingTimeMs(deadline) - retryDelayMs - responseTimeoutReserve.toMillis()
            if (availableMs <= 0) {
                dropByDeadline(paymentId, transactionId, "no time left for provider response")
                return Outcome()
            }
            Duration.ofMillis(minOf(processingTimeEstimate.toMillis(), availableMs))
        }
        if (!waitForRetry(retryDelayMs, deadline, admissionEstimate)) {
            dropByDeadline(paymentId, transactionId, "before external request")
            return Outcome()
        }

        if (!paymentCallAdmission.awaitCall(accountName, deadline, admissionEstimate)) {
            dropByDeadline(paymentId, transactionId, "waiting for account rate permit")
            return Outcome()
        }

        val remainingMs = remainingTimeMs(deadline)
        if (Thread.currentThread().isInterrupted || remainingMs <= 0) {
            dropByDeadline(paymentId, transactionId, "before external request")
            return Outcome()
        }

        val attemptBudget = if (providerAttemptTimeout.isZero) null else providerAttemptTimeout.plus(responseTimeoutReserve)
        val callTimeoutMs = minOf(remainingMs, attemptBudget?.toMillis() ?: remainingMs)
        val providerTimeoutMs = if (attemptBudget == null) callTimeoutMs else
            minOf(providerAttemptTimeout.toMillis(), callTimeoutMs - responseTimeoutReserve.toMillis())
        if (providerTimeoutMs <= 0) {
            dropByDeadline(paymentId, transactionId, "no time left for provider response")
            return Outcome()
        }

        val requestStartedAt = System.nanoTime()
        val outcome = try {
            val request = Request.Builder()
                .url(endpoint.newBuilder()
                    .addQueryParameter("serviceName", serviceName)
                    .addQueryParameter("token", token)
                    .addQueryParameter("accountName", accountName)
                    .addQueryParameter("transactionId", transactionId.toString())
                    .addQueryParameter("paymentId", paymentId.toString())
                    .addQueryParameter("amount", amount.toString())
                    .addQueryParameter("timeout", Duration.ofMillis(providerTimeoutMs).toString())
                    .build())
                .post(emptyBody)
                .build()
            val call = client.newCall(request)
            call.timeout().timeout(callTimeoutMs, TimeUnit.MILLISECONDS)
            retryReason?.let { retryCounters.getValue(it).increment() }
            call.execute().use(::readOutcome)
        } catch (exception: RetryableResponse) {
            exception.outcome
        } catch (exception: IOException) {
            Outcome(reason = "Unknown payment outcome: ${exception.javaClass.simpleName}", censored = exception is InterruptedIOException && !Thread.currentThread().isInterrupted)
        }

        val elapsedNanos = System.nanoTime() - requestStartedAt
        val elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos).coerceAtLeast(1)
        if (outcome.success || outcome.censored) {
            val observedMs = if (outcome.providerTimedOut) minOf(elapsedMs, providerTimeoutMs) else elapsedMs
            adaptiveTimeout?.observe(observedMs, outcome.censored)
        }
        Metrics.timer("payment_attempt_duration", metricTags.and("outcome", when {
            outcome.success -> "success"
            outcome.censored -> "timeout"
            else -> "error"
        })).record(elapsedNanos, TimeUnit.NANOSECONDS)

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${outcome.success}, message: ${outcome.reason}")

        paymentESService.update(paymentId) {
            it.logProcessing(outcome.success, now(), transactionId, reason = outcome.reason)
        }
        return outcome
    }

    private fun readOutcome(response: Response): Outcome {
        val rawBody = response.body?.string()
        val body = rawBody?.let {
            try {
                mapper.readValue(it, ExternalSysResponse::class.java)
            } catch (_: JsonProcessingException) {
                null
            }
        }
        if (!response.isSuccessful) {
            val confirmedTimeout = response.code == HttpStatus.REQUEST_TIMEOUT.value() &&
                body?.let { !it.result && it.message == "Timeout" } == true
            return Outcome(
                retryable = response.code in retryableStatuses &&
                    (response.code != HttpStatus.REQUEST_TIMEOUT.value() || confirmedTimeout),
                reason = "HTTP ${response.code}: ${body?.message ?: response.message}",
                retryDelayMs = retryAfterMillis(response.header("Retry-After")) ?: 0,
                retryReason = "http_${response.code}",
                censored = confirmedTimeout,
                providerTimedOut = confirmedTimeout,
            )
        }
        if (body == null) return Outcome(reason = "Invalid payment provider response")
        return Outcome(success = body.result, retryable = !body.result && body.message == "Temporary error", reason = body.message, retryReason = "temporary_error")
    }

    private fun retryAfterMillis(value: String?): Long? {
        if (value == null) return null
        val trimmed = value.trim()
        if (trimmed.matches(Regex("[0-9]+"))) {
            val seconds = trimmed.toLongOrNull() ?: return Long.MAX_VALUE
            return TimeUnit.SECONDS.toMillis(seconds)
        }
        return try {
            (ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli() - now())
                .coerceAtLeast(0)
        } catch (_: DateTimeParseException) {
            null
        }
    }

    private fun remainingTimeMs(deadline: Long): Long = deadline - now()

    private fun waitForRetry(delayMs: Long, deadline: Long, processingTimeEstimate: Duration): Boolean {
        val availableMs = deadline - now() - processingTimeEstimate.toMillis()
        if (Thread.currentThread().isInterrupted || availableMs <= 0 || delayMs >= availableMs) return false
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }
        return !Thread.currentThread().isInterrupted && deadline - now() > processingTimeEstimate.toMillis()
    }

    private fun dropByDeadline(paymentId: UUID, transactionId: UUID, msg: String) {
        deadlineDropped.increment()
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Deadline would be missed: $msg")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private data class Outcome(
        val success: Boolean = false,
        val retryable: Boolean = false,
        val reason: String? = null,
        val retryDelayMs: Long = 0,
        val retryReason: String? = null,
        val censored: Boolean = false,
        val providerTimedOut: Boolean = false,
    )
    private class RetryableResponse(val outcome: Outcome) : IOException("Retryable payment response")
}

fun now() = System.currentTimeMillis()
