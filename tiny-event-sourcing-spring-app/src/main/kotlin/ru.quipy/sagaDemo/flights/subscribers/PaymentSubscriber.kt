package ru.quipy.sagaDemo.flights.subscribers

import org.springframework.stereotype.Component
import ru.quipy.core.EventSourcingService
import ru.quipy.saga.SagaManager
import ru.quipy.sagaDemo.flights.api.FlightAggregate
import ru.quipy.sagaDemo.flights.logic.Flight
import ru.quipy.sagaDemo.payment.api.PaymentAggregate
import ru.quipy.sagaDemo.payment.api.PaymentSucceededEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class PaymentSubscriber (
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val flightEsService: EventSourcingService<UUID, FlightAggregate, Flight>,
    private val sagaManager: SagaManager
) {

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(PaymentAggregate::class, "flights::payment-subscriber") {
            `when`(PaymentSucceededEvent::class) { event ->
                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .launchSaga("TRIP_RESERVATION3", "reservation flight3")
                    .performSagaStep("TRIP_RESERVATION", "reservation flight")
                    .performSagaStep("TRIP_RESERVATION2", "reservation flight2")
                    .sagaContext()

                flightEsService.create(sagaContext) { it.reserveFlight(event.paymentId) }
            }
        }
    }
}