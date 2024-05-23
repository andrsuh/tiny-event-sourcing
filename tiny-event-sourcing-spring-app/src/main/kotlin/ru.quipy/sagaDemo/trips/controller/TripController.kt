package ru.quipy.sagaDemo.trips.controller

import org.springframework.web.bind.annotation.*
import ru.quipy.core.EventSourcingService
import ru.quipy.saga.SagaManager
import ru.quipy.sagaDemo.trips.api.TripAggregate
import ru.quipy.sagaDemo.trips.api.TripReservationStartedEvent
import ru.quipy.sagaDemo.trips.api.TripReservationFailedEvent
import ru.quipy.sagaDemo.trips.logic.Trip
import java.util.*

@RestController
class TripController(
    val tripEsService: EventSourcingService<UUID, TripAggregate, Trip>,
    val sagaManager: SagaManager,
) {

    @GetMapping
    fun reserveTrip() : TripReservationStartedEvent {
        val sagaContext = sagaManager
            .launchSaga("TRIP_RESERVATION", "start reservation")
            .sagaContext()

        return tripEsService.create(sagaContext) { it.startReservationTrip() }
    }

    @DeleteMapping("/{id}")
    fun cancelTrip(@PathVariable id: UUID) : TripReservationFailedEvent {
        return tripEsService.update(id) { it.cancelTrip(id) }
    }

    @GetMapping("/{id}")
    fun getAccount(@PathVariable id: UUID) : Trip? {
        return tripEsService.getState(id)
    }
}