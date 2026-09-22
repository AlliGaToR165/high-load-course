package ru.quipy.apigateway.ratelimit

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class AsyncPayOrderRequestQueueTest {
    @Test
    fun `queued request is executed after permit becomes available`() {
        val decisions = ArrayDeque(listOf(false, false, true))
        val queue = queue(
            policy = EndpointRateLimitPolicy {
                RateLimitDecision(decisions.removeFirst(), 1)
            }
        )
        val executed = CountDownLatch(1)

        try {
            val result = queue.submit {
                executed.countDown()
                "accepted"
            }

            assertTrue(executed.await(1, TimeUnit.SECONDS))
            assertEquals("accepted", result.result)
        } finally {
            queue.close()
        }
    }

    @Test
    fun `request is rejected when queue wait expires`() {
        val queue = queue(
            maxWait = Duration.ofMillis(20),
            policy = EndpointRateLimitPolicy { RateLimitDecision(false, 3) },
        )

        try {
            val result = queue.submit { "not executed" }

            assertTrue(waitForResult(result::hasResult))
            val response = result.result as ResponseEntity<*>
            assertEquals(HttpStatus.TOO_MANY_REQUESTS, response.statusCode)
            assertEquals("3", response.headers.getFirst(HttpHeaders.RETRY_AFTER))
        } finally {
            queue.close()
        }
    }

    @Test
    fun `queued pay order gets permit for every payment account`() {
        val checkedKeys = mutableListOf<RateLimitKey>()
        val queue = queue(
            accountNames = "acc-3,acc-4",
            policy = EndpointRateLimitPolicy { key ->
                checkedKeys.add(key)
                RateLimitDecision(true, 1)
            },
        )
        val executed = CountDownLatch(1)

        try {
            queue.submit {
                executed.countDown()
                "accepted"
            }

            assertTrue(executed.await(1, TimeUnit.SECONDS))
            assertEquals(
                listOf(
                    RateLimitKey("acc-3", ApiEndpoint.PAY_ORDER),
                    RateLimitKey("acc-4", ApiEndpoint.PAY_ORDER),
                ),
                checkedKeys,
            )
        } finally {
            queue.close()
        }
    }

    private fun queue(
        maxWait: Duration = Duration.ofSeconds(1),
        accountNames: String = "acc-3",
        policy: EndpointRateLimitPolicy,
    ): AsyncPayOrderRequestQueue {
        val properties = ApiRateLimitProperties().apply {
            payOrder.queueCapacity = 10
            payOrder.queueMaxWait = maxWait
            payOrder.queuePollInterval = Duration.ofMillis(1)
            payOrder.retryAfter = Duration.ofSeconds(3)
        }
        return AsyncPayOrderRequestQueue(
            policy,
            ConfiguredPaymentAccounts(accountNames),
            properties,
        )
    }

    private fun waitForResult(hasResult: () -> Boolean): Boolean {
        repeat(100) {
            if (hasResult()) return true
            Thread.sleep(5)
        }
        return false
    }
}
