package ru.quipy.sagaDemo.trips.subscribers

import org.springframework.stereotype.Component
import ru.quipy.core.EventSourcingService
import ru.quipy.saga.SagaManager
import ru.quipy.sagaDemo.payment.api.PaymentAggregate
import ru.quipy.sagaDemo.payment.api.PaymentCanceledEvent
import ru.quipy.sagaDemo.trips.api.TripAggregate
import ru.quipy.sagaDemo.trips.logic.Trip
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class PaymentTripSubscriber (
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val tripEsService: EventSourcingService<UUID, TripAggregate, Trip>,
    private val sagaManager: SagaManager
) {

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(PaymentAggregate::class, "trips::payment-subscriber") {
            `when`(PaymentCanceledEvent::class) { event ->
                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRIP_RESERVATION", "payment failed").sagaContext()

                tripEsService.update(event.paymentId, sagaContext) { it.cancelTrip(event.paymentId) }
            }
        }
    }
}