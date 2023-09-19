package ru.quipy.sagaDemo.payment.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType("payment")
class PaymentAggregate : Aggregate