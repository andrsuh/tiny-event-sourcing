package ru.quipy.demo

import ru.quipy.core.DomainEvent
import ru.quipy.domain.Event
import java.util.*

const val USER_CREATED_EVENT = "USER_CREATED_EVENT"
const val USER_ADDED_PAYMENT_EVENT = "USER_ADDED_PAYMENT_EVENT"
const val USER_ADDED_ADDRESS_EVENT = "USER_ADDED_ADDRESS_EVENT"
const val USER_CHANGED_PASSWORD_EVENT = "USER_CHANGED_PASSWORD_EVENT"
const val USER_SET_DEFAULT_ADDRESS_EVENT = "USER_SET_DEFAULT_ADDRESS_EVENT"
const val USER_SET_DEFAULT_PAYMENT_EVENT = "USER_SET_DEFAULT_PAYMENT_EVENT"

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