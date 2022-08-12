package ru.quipy.demo.domain

import ru.quipy.core.DomainEvent
import ru.quipy.domain.Event
import java.util.*

/**
 * Names of the events.
 *
 * It will be stores in DB with an event's payload. Later the reader of the event will read the name and use it to
 * find the corresponding class to deserialize payload to it.
 */

const val USER_CREATED_EVENT = "USER_CREATED_EVENT"
const val USER_ADDED_PAYMENT_EVENT = "USER_ADDED_PAYMENT_EVENT"
const val USER_ADDED_ADDRESS_EVENT = "USER_ADDED_ADDRESS_EVENT"
const val USER_CHANGED_PASSWORD_EVENT = "USER_CHANGED_PASSWORD_EVENT"
const val USER_SET_DEFAULT_ADDRESS_EVENT = "USER_SET_DEFAULT_ADDRESS_EVENT"
const val USER_SET_DEFAULT_PAYMENT_EVENT = "USER_SET_DEFAULT_PAYMENT_EVENT"

/**
 * Event - the fact of changing with the details. Will be persisted in DB.
 *
 * [applyTo] method takes the aggregate state and makes all the necessary changes for the update. Transits aggregate to
 * the next state.
 */
@DomainEvent(name = USER_CREATED_EVENT)
class UserCreatedEvent(
    val userLogin: String,
    val userPassword: String,
    val userName: String,
    val userId: String,
    createdAt: Long = System.currentTimeMillis(),
) : Event<UserAggregate>(
    name = USER_CREATED_EVENT,
    createdAt = createdAt,
    aggregateId = userId,
) {
    override fun applyTo(aggregate: UserAggregate) {
        aggregate.userLogin = userLogin
        aggregate.userPassword = userPassword
        aggregate.userName = userName
    }
}

/**
 * Event
 */
@DomainEvent(name = USER_ADDED_PAYMENT_EVENT)
class UserAddedPaymentEvent(
    val paymentMethod: String,
    val paymentMethodId: UUID,
    val userId: String
) : Event<UserAggregate>(
    name = USER_ADDED_PAYMENT_EVENT,
    aggregateId = userId
) {
    override fun applyTo(aggregate: UserAggregate) {
        aggregate.paymentMethods[paymentMethodId] = PaymentMethod(paymentMethodId, paymentMethod)
    }
}

/**
 * Event
 */
@DomainEvent(name = USER_ADDED_ADDRESS_EVENT)
class UserAddedAddressEvent(
    val address: String,
    val addressId: UUID,
    val userId: String
) : Event<UserAggregate>(
    name = USER_ADDED_ADDRESS_EVENT,
    aggregateId = userId
) {
    override fun applyTo(aggregate: UserAggregate) {
        aggregate.paymentMethods[addressId] = PaymentMethod(addressId, address)
    }
}

/**
 * Event
 */
@DomainEvent(name = USER_CHANGED_PASSWORD_EVENT)
class UserChangedPasswordEvent(
    val password: String,
    val userId: String
) : Event<UserAggregate>(
    name = USER_CHANGED_PASSWORD_EVENT,
    aggregateId = userId
) {
    override fun applyTo(aggregate: UserAggregate) {
        aggregate.userPassword = password
    }
}

/**
 * Event
 */
@DomainEvent(name = USER_SET_DEFAULT_ADDRESS_EVENT)
class UserSetDefaultAddressEvent(
    val addressId: UUID,
    val userId: String
) : Event<UserAggregate>(
    name = USER_SET_DEFAULT_ADDRESS_EVENT,
    aggregateId = userId
) {
    override fun applyTo(aggregate: UserAggregate) {
        aggregate.defaultAddressId = addressId
    }
}

/**
 * Event
 */
@DomainEvent(name = USER_SET_DEFAULT_PAYMENT_EVENT)
class UserSetDefaultPaymentEvent(
    val paymentMethodId: UUID,
    val userId: String
) : Event<UserAggregate>(
    name = USER_SET_DEFAULT_PAYMENT_EVENT,
    aggregateId = userId
) {
    override fun applyTo(aggregate: UserAggregate) {
        aggregate.defaultPaymentId = paymentMethodId
    }
}