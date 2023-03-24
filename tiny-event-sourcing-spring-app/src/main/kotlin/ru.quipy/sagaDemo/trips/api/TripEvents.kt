package ru.quipy.sagaDemo.trips.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.UUID

const val TRIP_RESERVATION_STARTED = "TRIP_RESERVATION_STARTED_EVENT"
const val TRIP_RESERVATION_FAILED = "TRIP_RESERVATION_FAILED_EVENT"
const val TRIP_RESERVATION_CONFIRMED = "TRIP_RESERVATION_CONFIRMED_EVENT"

@DomainEvent(TRIP_RESERVATION_STARTED)
data class TripReservationStartedEvent (
    val tripId: UUID
) : Event<TripAggregate>(
    name = TRIP_RESERVATION_STARTED,
)

@DomainEvent(TRIP_RESERVATION_FAILED)
data class TripReservationFailedEvent (
    val tripId: UUID
) : Event<TripAggregate>(
    name = TRIP_RESERVATION_FAILED,
)

@DomainEvent(TRIP_RESERVATION_CONFIRMED)
data class TripReservationConfirmedEvent (
    val tripId: UUID
) : Event<TripAggregate>(
    name = TRIP_RESERVATION_CONFIRMED,
)