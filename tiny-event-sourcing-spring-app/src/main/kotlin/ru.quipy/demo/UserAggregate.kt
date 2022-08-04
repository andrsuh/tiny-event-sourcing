package ru.quipy.demo

import ru.quipy.core.AggregateType
import ru.quipy.domain.Aggregate
import java.util.*

@AggregateType(aggregateEventsTableName = "aggregate-user")
data class UserAggregate(
    override val aggregateId: String
) : Aggregate {
    override var createdAt: Long = -1
    override var updatedAt: Long = System.currentTimeMillis()

    lateinit var userName: String
    lateinit var userLogin: String
    lateinit var userPassword: String
    lateinit var defaultPaymentId: UUID
    lateinit var defaultAddressId: UUID
    var paymentMethods = mutableMapOf<UUID, PaymentMethod>()
    var deliveryAddresses = mutableMapOf<UUID, DeliveryAddress>()
}

data class DeliveryAddress(
    val addressId: UUID,
    val address: String
)

data class PaymentMethod(
    val paymentMethodId: UUID,
    val cardNumber: String
)

fun UserAggregate.createUserCommand(
    name: String,
    password: String,
    login: String
): UserCreatedEvent {
    if (createdAt!=-1L){
        throw IllegalArgumentException("This event can't be called twice! aggregate id: $aggregateId")
    }
    if (name.length<3) {
        throw IllegalArgumentException("name is too small aggregate id: $aggregateId")
    }
    if (login.length<3) {
        throw IllegalArgumentException("login is too small aggregate id: $aggregateId")
    }
    if (password.isBlank()) {
        throw IllegalArgumentException("Can't change password: empty provided aggregate id: $aggregateId")
    }
    if (password.length < 8) {
        throw IllegalArgumentException("Password is too weak aggregate id: $aggregateId")
    }
    return UserCreatedEvent(
        userId = aggregateId,
        userLogin = login,
        userPassword = password,
        userName = name
    )
}

fun UserAggregate.addAddressCommand(
    address: String,
): UserAddedAddressEvent {
    if (address.isBlank()) {
        throw IllegalArgumentException("Can't add address: blank provided aggregate id: $aggregateId")
    }
    if (address.isEmpty()) {
        throw IllegalArgumentException("Can't add address: empty provided aggregate id: $aggregateId")
    }
    return UserAddedAddressEvent(
        address = address,
        addressId = UUID.randomUUID(),
        userId = aggregateId
    )
}

fun UserAggregate.changePasswordCommand(
    password: String,
): UserChangedPasswordEvent {
    if (password == this.userPassword) {
        throw IllegalArgumentException("Can't change password: previous password aggregate id: $aggregateId")
    }
    if (password.isBlank()) {
        throw IllegalArgumentException("Can't change password: empty provided aggregate id: $aggregateId")
    }
    if (password.length < 8) {
        throw IllegalArgumentException("Password is too weak aggregate id: $aggregateId")
    }
    return UserChangedPasswordEvent(
        password = password,
        userId = aggregateId
    )
}

fun UserAggregate.setDefaultAddressCommand(
    addressId: UUID,
): UserSetDefaultAddressEvent {
    if (!this.deliveryAddresses.contains(addressId)) {
        throw IllegalArgumentException("There is no such address $addressId in aggregate id: $aggregateId")
    }
    return UserSetDefaultAddressEvent(
        addressId = addressId,
        userId = aggregateId
    )
}

fun UserAggregate.setDefaultPaymentCommand(
    paymentMethodId: UUID,
): UserSetDefaultPaymentEvent {
    if (!this.deliveryAddresses.contains(paymentMethodId)) {
        throw IllegalArgumentException("There is no such address $paymentMethodId in aggregate id: $aggregateId")
    }
    return UserSetDefaultPaymentEvent(
        paymentMethodId = paymentMethodId,
        userId = aggregateId
    )
}