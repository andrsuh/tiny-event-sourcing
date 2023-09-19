package ru.quipy.sagaDemo.trips.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType("trips")
class TripAggregate : Aggregate