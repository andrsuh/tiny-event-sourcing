package ru.quipy.sagaDemo.payment.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.UUID

const val PAYMENT_SUCCEEDED = "PAYMENT_SUCCEEDED_EVENT"
const val PAYMENT_CANCELED = "PAYMENT_CANCELED_EVENT"

@DomainEvent(PAYMENT_SUCCEEDED)
data class PaymentSucceededEvent (
    val paymentId: UUID,
    val amount: Int
) : Event<PaymentAggregate>(
    name = PAYMENT_SUCCEEDED,
)

@DomainEvent(PAYMENT_CANCELED)
data class PaymentCanceledEvent (
    val paymentId: UUID,
) : Event<PaymentAggregate>(
    name = PAYMENT_CANCELED,
)