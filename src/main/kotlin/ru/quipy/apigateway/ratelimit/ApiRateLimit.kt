package ru.quipy.apigateway.ratelimit

import jakarta.annotation.PreDestroy
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import org.springframework.web.method.HandlerMethod
import org.springframework.web.servlet.HandlerInterceptor
import org.springframework.web.servlet.config.annotation.InterceptorRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import org.springframework.web.context.request.async.DeferredResult
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

enum class ApiEndpoint {
    AUTHENTICATION,
    AUTHENTICATION_REFRESH,
    CREATE_USER,
    CREATE_ORDER,
    PAY_ORDER,
}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RateLimited(val endpoint: ApiEndpoint)

class EndpointRateLimitProperties {
    var rate: Long = 100
    var window: Duration = Duration.ofSeconds(1)
    var retryAfter: Duration = Duration.ofSeconds(1)
    var queueCapacity: Int = 2_000
    var queueMaxWait: Duration = Duration.ofSeconds(70)
    var queuePollInterval: Duration = Duration.ofMillis(10)
}

@ConfigurationProperties(prefix = "api.rate-limit")
class ApiRateLimitProperties {
    var authentication = EndpointRateLimitProperties()
    var authenticationRefresh = EndpointRateLimitProperties()
    var createUser = EndpointRateLimitProperties()
    var createOrder = EndpointRateLimitProperties()
    var payOrder = EndpointRateLimitProperties()
}

data class RateLimitDecision(
    val allowed: Boolean,
    val retryAfterSeconds: Long,
)

data class RateLimitKey(
    val accountName: String,
    val endpoint: ApiEndpoint,
)

data class ConfiguredEndpointRateLimiter(
    val rateLimiter: RateLimiter,
    val retryAfterSeconds: Long,
)

fun interface EndpointRateLimitPolicy {
    fun check(key: RateLimitKey): RateLimitDecision
}

@Component
class ConfiguredPaymentAccounts(
    @Value("\${payment.accounts}") accountNames: String,
) {
    val names = accountNames.split(',')
        .map(String::trim)
        .filter(String::isNotEmpty)
        .distinct()

    init {
        require(names.isNotEmpty()) { "At least one payment account must be configured" }
    }
}

interface PayOrderRequestQueue {
    fun <T : Any> submit(action: () -> T): DeferredResult<T>
}

@Component
class AsyncPayOrderRequestQueue(
    private val rateLimitPolicy: EndpointRateLimitPolicy,
    private val paymentAccounts: ConfiguredPaymentAccounts,
    properties: ApiRateLimitProperties,
) : PayOrderRequestQueue, AutoCloseable {
    private val settings = properties.payOrder
    private val requests: ArrayBlockingQueue<QueuedRequest>
    private val dispatcher = Executors.newSingleThreadExecutor(NamedThreadFactory("pay-order-rate-limit-queue"))
    private val closed = AtomicBoolean(false)

    init {
        require(settings.queueCapacity > 0) { "Pay order queue capacity must be positive" }
        require(!settings.queueMaxWait.isZero && !settings.queueMaxWait.isNegative) {
            "Pay order queue max wait must be positive"
        }
        require(!settings.queuePollInterval.isZero && !settings.queuePollInterval.isNegative) {
            "Pay order queue poll interval must be positive"
        }

        requests = ArrayBlockingQueue(settings.queueCapacity)
        dispatcher.execute(::dispatchRequests)
    }

    override fun <T : Any> submit(action: () -> T): DeferredResult<T> {
        val result = DeferredResult<T>(settings.queueMaxWait.toMillis())
        val request = QueuedRequest(
            expiresAtNanos = System.nanoTime() + settings.queueMaxWait.toNanos(),
            executeAction = {
                try {
                    result.setResult(action())
                } catch (exception: Throwable) {
                    result.setErrorResult(exception)
                }
            },
            rejectAction = {
                result.setErrorResult(rateLimitResponse())
            },
        )

        result.onTimeout(request::reject)
        result.onCompletion(request::cancel)

        if (!requests.offer(request)) {
            request.reject()
        }
        return result
    }

    private fun dispatchRequests() {
        try {
            while (!closed.get()) {
                awaitPermitAndExecute(requests.take())
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun awaitPermitAndExecute(request: QueuedRequest) {
        while (request.isActive()) {
            val remainingNanos = request.expiresAtNanos - System.nanoTime()
            if (remainingNanos <= 0) {
                request.reject()
                return
            }

            val allAccountsAllowed = paymentAccounts.names
                .asSequence()
                .map { accountName -> rateLimitPolicy.check(RateLimitKey(accountName, ApiEndpoint.PAY_ORDER)) }
                .all(RateLimitDecision::allowed)
            if (allAccountsAllowed) {
                request.execute()
                return
            }

            TimeUnit.NANOSECONDS.sleep(minOf(settings.queuePollInterval.toNanos(), remainingNanos))
        }
    }

    private fun rateLimitResponse(): ResponseEntity<Void> {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, settings.retryAfter.seconds.toString())
            .build()
    }

    @PreDestroy
    override fun close() {
        if (closed.compareAndSet(false, true)) {
            dispatcher.shutdownNow()
            requests.forEach(QueuedRequest::reject)
            requests.clear()
        }
    }

    private class QueuedRequest(
        val expiresAtNanos: Long,
        private val executeAction: () -> Unit,
        private val rejectAction: () -> Unit,
    ) {
        private val active = AtomicBoolean(true)

        fun isActive(): Boolean = active.get()

        fun execute() {
            if (active.compareAndSet(true, false)) executeAction()
        }

        fun reject() {
            if (active.compareAndSet(true, false)) rejectAction()
        }

        fun cancel() {
            active.set(false)
        }
    }
}

class EndpointRateLimiterRegistry(
    private val limiterFactories: Map<ApiEndpoint, () -> ConfiguredEndpointRateLimiter>,
) : EndpointRateLimitPolicy {
    private val limiters = ConcurrentHashMap<RateLimitKey, ConfiguredEndpointRateLimiter>()

    override fun check(key: RateLimitKey): RateLimitDecision {
        val limiter = limiters.computeIfAbsent(key) {
            limiterFactories.getValue(key.endpoint).invoke()
        }
        return RateLimitDecision(
            limiter.rateLimiter.tick(),
            limiter.retryAfterSeconds,
        )
    }
}

@Component
class RateLimitInterceptor(
    private val rateLimitPolicy: EndpointRateLimitPolicy,
    private val paymentAccounts: ConfiguredPaymentAccounts,
) : HandlerInterceptor {
    override fun preHandle(
        request: HttpServletRequest,
        response: HttpServletResponse,
        handler: Any,
    ): Boolean {
        val endpoint = (handler as? HandlerMethod)
            ?.getMethodAnnotation(RateLimited::class.java)
            ?.endpoint
            ?: return true

        if (endpoint == ApiEndpoint.PAY_ORDER) return true

        val rejection = paymentAccounts.names
            .asSequence()
            .map { accountName -> rateLimitPolicy.check(RateLimitKey(accountName, endpoint)) }
            .firstOrNull { !it.allowed }
            ?: return true

        response.status = HttpStatus.TOO_MANY_REQUESTS.value()
        response.setHeader(HttpHeaders.RETRY_AFTER, rejection.retryAfterSeconds.toString())
        return false
    }
}

@Configuration
@EnableConfigurationProperties(ApiRateLimitProperties::class)
class ApiRateLimitConfiguration {
    @Bean
    fun endpointRateLimitPolicy(properties: ApiRateLimitProperties): EndpointRateLimitPolicy {
        return EndpointRateLimiterRegistry(
            mapOf(
                ApiEndpoint.AUTHENTICATION to properties.authentication.toRateLimiterFactory(),
                ApiEndpoint.AUTHENTICATION_REFRESH to properties.authenticationRefresh.toRateLimiterFactory(),
                ApiEndpoint.CREATE_USER to properties.createUser.toRateLimiterFactory(),
                ApiEndpoint.CREATE_ORDER to properties.createOrder.toRateLimiterFactory(),
                ApiEndpoint.PAY_ORDER to properties.payOrder.toRateLimiterFactory(),
            )
        )
    }

    private fun EndpointRateLimitProperties.toRateLimiterFactory(): () -> ConfiguredEndpointRateLimiter {
        require(rate > 0) { "Rate limit must be positive" }
        require(!window.isZero && !window.isNegative) { "Rate limit window must be positive" }
        require(retryAfter.seconds > 0 && retryAfter.nano == 0) {
            "Retry-After must be a positive whole number of seconds"
        }

        val configuredRate = rate
        val configuredWindow = window
        val configuredRetryAfterSeconds = retryAfter.seconds
        return {
            ConfiguredEndpointRateLimiter(
                SlidingWindowRateLimiter(configuredRate, configuredWindow),
                configuredRetryAfterSeconds,
            )
        }
    }
}

@Configuration
class RateLimitWebConfiguration(
    private val rateLimitInterceptor: RateLimitInterceptor,
) : WebMvcConfigurer {
    override fun addInterceptors(registry: InterceptorRegistry) {
        registry.addInterceptor(rateLimitInterceptor)
    }
}
