package ru.quipy.core

import ru.quipy.database.EventStore
import ru.quipy.domain.*
import ru.quipy.mapper.EventMapper


class EventSourcingServiceFactory(
    val aggregateRegistry: AggregateRegistry,
    val eventMapper: EventMapper,
    val eventStore: EventStore,
    val eventSourcingProperties: EventSourcingProperties
) {

    inline fun <ID : Any, reified A : Aggregate, S: AggregateState<ID, A>> create(): EventSourcingService<ID, A, S> {
        return EventSourcingService(
            A::class, aggregateRegistry, eventMapper, eventSourcingProperties, eventStore
        )
    }
}