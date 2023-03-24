package ru.quipy.sagaDemo.payment.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.domain.Event
import ru.quipy.sagaDemo.payment.api.PaymentAggregate
import ru.quipy.sagaDemo.payment.api.PaymentCanceledEvent
import ru.quipy.sagaDemo.payment.api.PaymentSucceededEvent
import java.util.*

class Payment : AggregateState<UUID, PaymentAggregate> {
    private lateinit var paymentId: UUID
    private var failed: Boolean = false

    override fun getId() = paymentId

    fun processPayment(id: UUID = UUID.randomUUID(), amount: Int) : Event<PaymentAggregate> {
        return if (amount > 0)
            PaymentSucceededEvent(id, amount)
        else
            PaymentCanceledEvent(id)
    }

    fun canselPayment(id: UUID) : PaymentCanceledEvent {
        return PaymentCanceledEvent(id)
    }

    @StateTransitionFunc
    fun processPayment(event: PaymentSucceededEvent) {
        paymentId = event.paymentId
    }

    @StateTransitionFunc
    fun canselPayment(event: PaymentCanceledEvent) {
        paymentId = event.paymentId
        failed = true
    }
}