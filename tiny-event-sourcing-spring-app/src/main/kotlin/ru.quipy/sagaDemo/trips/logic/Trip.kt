package ru.quipy.sagaDemo.trips.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.sagaDemo.trips.api.TripAggregate
import ru.quipy.sagaDemo.trips.api.TripReservationConfirmedEvent
import ru.quipy.sagaDemo.trips.api.TripReservationStartedEvent
import ru.quipy.sagaDemo.trips.api.TripReservationFailedEvent
import java.util.*

class Trip : AggregateState<UUID, TripAggregate> {
    private lateinit var tripId: UUID
    private var canceled: Boolean = false
    private var confirmed: Boolean = false

    override fun getId() = tripId

    fun startReservationTrip(id: UUID = UUID.randomUUID()) : TripReservationStartedEvent {
        return TripReservationStartedEvent(id)
    }

    fun cancelTrip(id: UUID) : TripReservationFailedEvent {
        return TripReservationFailedEvent(id)
    }

    fun confirmTrip(id: UUID) : TripReservationConfirmedEvent {
        return TripReservationConfirmedEvent(id)
    }

    @StateTransitionFunc
    fun startReservationTrip(event: TripReservationStartedEvent) {
        tripId = event.tripId
    }

    @StateTransitionFunc
    fun cancelTrip(event: TripReservationFailedEvent) {
        canceled = true
    }

    @StateTransitionFunc
    fun confirmTrip(event: TripReservationConfirmedEvent) {
        confirmed = true
    }
}