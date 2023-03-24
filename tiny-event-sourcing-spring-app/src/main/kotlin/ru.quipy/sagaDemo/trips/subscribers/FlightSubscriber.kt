package ru.quipy.sagaDemo.trips.subscribers

import org.springframework.stereotype.Component
import ru.quipy.core.EventSourcingService
import ru.quipy.saga.SagaManager
import ru.quipy.sagaDemo.flights.api.FlightAggregate
import ru.quipy.sagaDemo.flights.api.FlightReservedEvent
import ru.quipy.sagaDemo.trips.api.TripAggregate
import ru.quipy.sagaDemo.trips.logic.Trip
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class FlightSubscriber (
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val tripEsService: EventSourcingService<UUID, TripAggregate, Trip>,
    private val sagaManager: SagaManager
) {

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(FlightAggregate::class, "trips::flights-subscriber") {
            `when`(FlightReservedEvent::class) { event ->
                val sagaStep = sagaManager.performSagaStep("TRIP_RESERVATION", "finish reservation", event.sagaContext)
                tripEsService.update(event.flightReservationId, sagaStep) { it.confirmTrip(event.flightReservationId) }
            }
        }
    }
}