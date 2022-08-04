package ru.quipy.demo

import org.slf4j.*
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import ru.quipy.demo.*
import ru.quipy.domain.Unique
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct


class UserPaymentsViewDomain {
    @Document("user-payment-view")
    data class UserPayments(
        @Id
        override val id: String, // userId
        var defaultPaymentMethodId: UUID? = null, //Id of favorite payment
        val paymentMethods: MutableMap<UUID, Payment> = mutableMapOf() // map to hold all payments
    ) : Unique<String>

    data class Payment(
        val paymentId: UUID,
        val payment: String
    )
}

@Service
class UserPaymentsViewService(
    private val userPaymentsRepository: UserPaymentsRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager
) {
    private val logger: Logger = LoggerFactory.getLogger(UserPaymentsViewService::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(UserAggregate::class, "userPayments-payment-event-publisher-stream") {
            `when`(UserCreatedEvent::class) { event ->
                createUserPayment(event.userId, event.userName)
                logger.info("User Payment Created {}", event.userName)
            }
            `when`(UserAddedPaymentEvent::class) { event ->
                addPayment(event.paymentMethodId, event.userId, event.paymentMethod)
                logger.info("User Payment added {}", event.paymentMethod)
            }
            `when`(UserSetDefaultPaymentEvent::class) { event ->
                setDefaultPayment(event.userId, event.paymentMethodId)
                logger.info("Default User Payment selected {}", event.paymentMethodId)
            }
        }
    }

    private fun setDefaultPayment(userId: String, paymentMethodId: UUID) {
        val userPayments = userPaymentsRepository.findById(userId).orElse(
            UserPaymentsViewDomain.UserPayments(userId)
        )
        userPayments.defaultPaymentMethodId = paymentMethodId
    }


    private fun createUserPayment(userId: String, userName: String) {
        val userPayments = UserPaymentsViewDomain.UserPayments(userId)
        userPaymentsRepository.save(userPayments)
    }

    private fun addPayment(paymentMethodId: UUID, userId: String, paymentMethod: String) {
        val userPayments = userPaymentsRepository.findById(userId).orElse(
            UserPaymentsViewDomain.UserPayments(userId)
        )
        userPayments.paymentMethods.put(paymentMethodId, UserPaymentsViewDomain.Payment(paymentMethodId, paymentMethod))
    }
}

@Repository
interface UserPaymentsRepository : MongoRepository<UserPaymentsViewDomain.UserPayments, String> {
}