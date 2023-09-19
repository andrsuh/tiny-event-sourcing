package ru.quipy.sagaDemo.flights.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType("flights")
class FlightAggregate : Aggregate