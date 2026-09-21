package ru.quipy.apigateway.ratelimit

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import org.springframework.web.method.HandlerMethod
import ru.quipy.common.utils.RateLimiter

class RateLimitInterceptorTest {
    @Test
    fun `request is passed when endpoint has available permit`() {
        val interceptor = RateLimitInterceptor(
            EndpointRateLimitPolicy { RateLimitDecision(true, 1) }
        )
        val response = MockHttpServletResponse()

        val passed = interceptor.preHandle(
            MockHttpServletRequest(),
            response,
            handler("createUser"),
        )

        assertTrue(passed)
        assertEquals(200, response.status)
    }

    @Test
    fun `request is rejected with retry after header when limit is exceeded`() {
        val interceptor = RateLimitInterceptor(
            EndpointRateLimitPolicy { RateLimitDecision(false, 3) }
        )
        val response = MockHttpServletResponse()

        val passed = interceptor.preHandle(
            MockHttpServletRequest(),
            response,
            handler("payOrder"),
        )

        assertFalse(passed)
        assertEquals(429, response.status)
        assertEquals("3", response.getHeader(HttpHeaders.RETRY_AFTER))
    }

    @Test
    fun `different endpoints use independent rate limiters`() {
        val createUserLimiter = TestRateLimiter(listOf(true, false))
        val payOrderLimiter = TestRateLimiter(listOf(true, true))
        val registry = EndpointRateLimiterRegistry(
            mapOf(
                ApiEndpoint.CREATE_USER to ConfiguredEndpointRateLimiter(createUserLimiter, 1),
                ApiEndpoint.PAY_ORDER to ConfiguredEndpointRateLimiter(payOrderLimiter, 2),
            )
        )

        assertTrue(registry.check(ApiEndpoint.CREATE_USER).allowed)
        assertFalse(registry.check(ApiEndpoint.CREATE_USER).allowed)
        assertTrue(registry.check(ApiEndpoint.PAY_ORDER).allowed)
        val payOrderDecision = registry.check(ApiEndpoint.PAY_ORDER)
        assertTrue(payOrderDecision.allowed)
        assertEquals(2, payOrderDecision.retryAfterSeconds)
    }

    @Test
    fun `unannotated handler is not rate limited`() {
        val interceptor = RateLimitInterceptor(
            EndpointRateLimitPolicy { RateLimitDecision(false, 1) }
        )

        val passed = interceptor.preHandle(
            MockHttpServletRequest(),
            MockHttpServletResponse(),
            handler("unlimited"),
        )

        assertTrue(passed)
    }

    private fun handler(methodName: String): HandlerMethod {
        return HandlerMethod(
            TestController(),
            TestController::class.java.getDeclaredMethod(methodName),
        )
    }

    private class TestController {
        @RateLimited(ApiEndpoint.CREATE_USER)
        fun createUser() = Unit

        @RateLimited(ApiEndpoint.PAY_ORDER)
        fun payOrder() = Unit

        fun unlimited() = Unit
    }

    private class TestRateLimiter(
        results: List<Boolean>,
    ) : RateLimiter {
        private val results = ArrayDeque(results)

        override fun tick(): Boolean {
            return results.removeFirst()
        }
    }
}
