package ru.quipy.user.api

import org.apache.catalina.User
import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.*

const val USER_CREATED = "USER_CREATED_EVENT"
const val USER_CREATED_CART = "USER_CREATED_CART"
const val USER_RESET_CART = "USER_RESET_CART"
const val USER_ADD_TRACK = "USER_ADD_TRACK"

@DomainEvent(name = USER_CREATED)
data class UserCreatedEvent(
    val userId: UUID,
) : Event<UserAggregate>(
    name = USER_CREATED
)

@DomainEvent(name = USER_CREATED_CART)
data class UserCreatedCartEvent(
    val userId: UUID,
    val cartId: UUID,
) : Event<UserAggregate>(
    name = USER_CREATED_CART
)

@DomainEvent(name = USER_RESET_CART)
data class UserResetCartEvent(
    val userId: UUID,
    val cartId: UUID
) : Event<UserAggregate>(
    name = USER_RESET_CART,
)

@DomainEvent(name = USER_ADD_TRACK)
data class UserAddTrackEvent(
    val userId: UUID,
    val trackId: UUID
) : Event<UserAggregate>(
    name = USER_ADD_TRACK,
)