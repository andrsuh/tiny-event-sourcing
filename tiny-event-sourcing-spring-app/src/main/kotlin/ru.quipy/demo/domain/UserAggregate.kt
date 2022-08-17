package ru.quipy.demo.domain

import ru.quipy.core.AggregateType
import ru.quipy.domain.Aggregate
import java.util.*

/**
 * In our domain we have user. User of our "Online shopping" system. We would like to know his name, login, password etc.
 * Also, we would like to maintain the list of possible delivery addresses for the user and list of
 * his payment methods. Let him add/remove addresses and payment methods and choose the ones that will be
 * used by default when he makes an order.
 *
 * Each address and payment method is a separate entity with their own ID. You can see that the structure is denormolized.
 * This example is farfetched a bit. Addresses and payment methods might have been separate aggregates. But it gives you
 * benefits of atomic updates. For example, you can atomically add new delivery address and set it as default for the user.
 *
 * Represents the state of the UserAggregate at different points of time.
 *
 * This structure is used:
 * - To apply changes (events) to it and reproduce the states of the aggregate at different points of time
 * - To apply commands. Command - some method that takes current aggregate state, perform all checks and validations
 * and produces event if changes is allowed and legit.
 */
@AggregateType(aggregateEventsTableName = "aggregate-user")
data class UserAggregate(
    override val aggregateId: String
) : Aggregate {
    override var createdAt: Long = -1
    override var updatedAt: Long = System.currentTimeMillis()

    // Fields should be mutable or mutable collections. This container will be used by internal library logic to
    // construct current aggregate state

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

/**
 * This is command. Method that takes current aggregate state, perform all checks and validations
 * and produces event if changes is allowed and legit
 */
fun UserAggregate.createUserCommand(
    name: String,
    password: String,
    login: String
): UserCreatedEvent {
    if (createdAt != -1L) {
        throw IllegalArgumentException("This event can't be called twice! aggregate id: $aggregateId")
    }
    if (name.length < 3) {
        throw IllegalArgumentException("Name is too small aggregate id: $aggregateId")
    }
    if (login.length < 3) {
        throw IllegalArgumentException("Login is too small aggregate id: $aggregateId")
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

/**
 * Command
 */
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

/**
 * Command
 */
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

/**
 * Command
 */
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

/**
 * Command
 */
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