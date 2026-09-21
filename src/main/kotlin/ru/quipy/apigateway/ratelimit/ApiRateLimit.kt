package ru.quipy.apigateway.ratelimit

import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.method.HandlerMethod
import org.springframework.web.servlet.HandlerInterceptor
import org.springframework.web.servlet.config.annotation.InterceptorRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration

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

data class ConfiguredEndpointRateLimiter(
    val rateLimiter: RateLimiter,
    val retryAfterSeconds: Long,
)

fun interface EndpointRateLimitPolicy {
    fun check(endpoint: ApiEndpoint): RateLimitDecision
}

class EndpointRateLimiterRegistry(
    private val limiters: Map<ApiEndpoint, ConfiguredEndpointRateLimiter>,
) : EndpointRateLimitPolicy {
    override fun check(endpoint: ApiEndpoint): RateLimitDecision {
        val limiter = limiters.getValue(endpoint)
        return RateLimitDecision(
            limiter.rateLimiter.tick(),
            limiter.retryAfterSeconds,
        )
    }
}

@Component
class RateLimitInterceptor(
    private val rateLimitPolicy: EndpointRateLimitPolicy,
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

        val decision = rateLimitPolicy.check(endpoint)
        if (decision.allowed) {
            return true
        }

        response.status = HttpStatus.TOO_MANY_REQUESTS.value()
        response.setHeader(HttpHeaders.RETRY_AFTER, decision.retryAfterSeconds.toString())
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
                ApiEndpoint.AUTHENTICATION to properties.authentication.toRateLimiter(),
                ApiEndpoint.AUTHENTICATION_REFRESH to properties.authenticationRefresh.toRateLimiter(),
                ApiEndpoint.CREATE_USER to properties.createUser.toRateLimiter(),
                ApiEndpoint.CREATE_ORDER to properties.createOrder.toRateLimiter(),
                ApiEndpoint.PAY_ORDER to properties.payOrder.toRateLimiter(),
            )
        )
    }

    private fun EndpointRateLimitProperties.toRateLimiter(): ConfiguredEndpointRateLimiter {
        require(rate > 0) { "Rate limit must be positive" }
        require(!window.isZero && !window.isNegative) { "Rate limit window must be positive" }
        require(retryAfter.seconds > 0 && retryAfter.nano == 0) {
            "Retry-After must be a positive whole number of seconds"
        }
        return ConfiguredEndpointRateLimiter(
            SlidingWindowRateLimiter(rate, window),
            retryAfter.seconds,
        )
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
