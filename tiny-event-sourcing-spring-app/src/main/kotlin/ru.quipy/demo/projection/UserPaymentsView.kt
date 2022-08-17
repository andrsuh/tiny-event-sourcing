package ru.quipy.demo.projection

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import ru.quipy.demo.domain.UserAddedPaymentEvent
import ru.quipy.demo.domain.UserAggregate
import ru.quipy.demo.domain.UserCreatedEvent
import ru.quipy.demo.domain.UserSetDefaultPaymentEvent
import ru.quipy.domain.Unique
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

/**
 * Imagine that we have separate microservice in our system that is responsible for performing payments.
 *
 * It wants to maintain local cache (in DB, not in-memory) that will contain the only information about
 * user's payment methods. This local cache is derived from event stream of [UserAggregate]. We will listen to
 * some events and update our local DB. The local cache also known as **Projection** or **View**
 *
 * In this case we store documents that holds the payment methods for each user.
 */
class UserPaymentsViewDomain {
    @Document("user-payment-view")
    data class UserPayments(
        @Id
        override val id: String, // userId
        var defaultPaymentMethodId: UUID? = null, // what to use by default
        val paymentMethods: MutableMap<UUID, Payment> = mutableMapOf() // map to hold all payments
    ) : Unique<String>

    data class Payment(
        val paymentId: UUID,
        val payment: String
    )
}

/**
 * Subscribes to the [UserAggregate] events stream. It's interested in only tree types of the events:
 * [UserCreatedEvent], [UserAddedPaymentEvent], [UserSetDefaultPaymentEvent]. According to event type
 * it updates its local DB.
 */
@Service
class UserPaymentsViewService(
    private val userPaymentsRepository: UserPaymentsRepository,

    /**
     * Allows you to create the subscriber and define the event handlers.
     */
    private val subscriptionsManager: AggregateSubscriptionsManager
) {
    private val logger: Logger = LoggerFactory.getLogger(UserPaymentsViewService::class.java)

    @PostConstruct
    fun init() {
        // Subscribes to UserAggregate event stream. Subscriber has its own name to store its state to DB.
        subscriptionsManager.createSubscriber(UserAggregate::class, "userPayments-payment-event-publisher-stream") {

            /**
             * Allows you to define the event handler for certain type of events. In this case for [UserCreatedEvent]
             */
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
interface UserPaymentsRepository : MongoRepository<UserPaymentsViewDomain.UserPayments, String>