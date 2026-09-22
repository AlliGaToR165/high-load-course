package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import org.springframework.web.context.request.async.DeferredResult
import ru.quipy.apigateway.ratelimit.ApiEndpoint
import ru.quipy.apigateway.ratelimit.PayOrderRequestQueue
import ru.quipy.apigateway.ratelimit.RateLimited
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*

@RestController
class APIController(
    private val payOrderRequestQueue: PayOrderRequestQueue,
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    @RateLimited(ApiEndpoint.CREATE_USER)
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    @RateLimited(ApiEndpoint.CREATE_ORDER)
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    @RateLimited(ApiEndpoint.PAY_ORDER)
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): DeferredResult<PaymentSubmissionDto> {
        return payOrderRequestQueue.submit {
            val paymentId = UUID.randomUUID()
            val order = orderRepository.findById(orderId)?.let {
                orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
                it
            } ?: throw IllegalArgumentException("No such order $orderId")


            val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
            PaymentSubmissionDto(createdAt, paymentId)
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
