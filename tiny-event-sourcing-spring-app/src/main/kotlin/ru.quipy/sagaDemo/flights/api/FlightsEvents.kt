package ru.quipy.sagaDemo.flights.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.UUID

const val FLIGHTS_RESERVED = "FLIGHTS_RESERVED_EVENT"
const val FLIGHTS_RESERVATION_FAILED = "FLIGHTS_RESERVATION_FAILED_EVENT"

@DomainEvent(FLIGHTS_RESERVED)
data class FlightReservedEvent (
    val flightReservationId: UUID
) : Event<FlightAggregate>(
    name = FLIGHTS_RESERVED,
)

@DomainEvent(FLIGHTS_RESERVATION_FAILED)
data class FlightReservationCanceledEvent (
    val flightReservationId: UUID
): Event<FlightAggregate>(
    name = FLIGHTS_RESERVATION_FAILED,
)