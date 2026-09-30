package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val window = Semaphore(parallelRequests, true) // true - честный семафор, т.е раньше пришел -> раньше получил место в очереди

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val metricTags = Tags.of("account", accountName)
    private val deadlineDropped = Metrics.counter("payment_deadline_dropped", metricTags) // счетчик, сколько запросов реджектнули из-за просрочки дедлайна
    private val windowWaitTimer = Timer.builder("payment_window_wait") // таймер, сколько ждали свободного места в семафоре
        .tags(metricTags)
        .register(Metrics.globalRegistry)
    private val waitingForWindow = AtomicInteger(0)
    init {
        Metrics.gauge("payment_waiting_for_window", metricTags, waitingForWindow) { it.get().toDouble() } //  датчик, сколько щас ждет очереди в семафоре
        Metrics.gauge("payment_inflight", metricTags, window) { (parallelRequests - it.availablePermits()).toDouble() } // датчик, сколько щас занято мест в семафоре
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Если оставшегося времени не хватает даже на обработку, то т.к + ожидание места в семафора, по времени точно не успеем
        if (!hasEnoughTime(deadline)) {
            dropByDeadline(paymentId, transactionId, "before waiting for window")
            return
        }

        // Если ждем места в семафоре больше максимального допустимого времени (оставшееся время - время на обработку запроса), тоже не успеваем
        if (!acquireWindow(deadline)) {
            dropByDeadline(paymentId, transactionId, "no free window slot in time")
            return
        }

        try {
            rateLimiter.tickBlocking() // Если в текущую секунду уже >= rateLimitPerSec запросов, осташиеся в семафоре ждут

            // Если, пока ждали очереди из-за лимитера, не хватает времени, то падаем
            if (!hasEnoughTime(deadline)) {
                dropByDeadline(paymentId, transactionId, "after waiting for rate limiter")
                return
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            window.release()
        }
    }

    private fun hasEnoughTime(deadline: Long): Boolean {
        return deadline - now() >= requestAverageProcessingTime.toMillis()
    }

    private fun acquireWindow(deadline: Long): Boolean {
        val remainingTime = deadline - now()

        // Это максимум, который имеет смысл ждать. Если не дождались за это время места в очереди, нет смысла обрабатывать дальше запрос
        val maxWaitMillis = remainingTime - requestAverageProcessingTime.toMillis()
        val waitStartedAt = System.nanoTime()
        waitingForWindow.incrementAndGet() // увеличиваем датчик в моменте, т.к +1 запрос начинает ждать в очереди
        try {
            return window.tryAcquire(maxWaitMillis, TimeUnit.MILLISECONDS)
        } finally {
            waitingForWindow.decrementAndGet() // уменьшаем датчик, т.к больше не ждем
            windowWaitTimer.record(System.nanoTime() - waitStartedAt, TimeUnit.NANOSECONDS)
        }
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

}

public fun now() = System.currentTimeMillis()