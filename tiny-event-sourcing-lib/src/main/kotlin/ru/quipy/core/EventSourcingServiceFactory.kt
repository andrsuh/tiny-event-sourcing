package ru.quipy.core

import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*
import ru.quipy.mapper.EventMapper
import kotlin.reflect.KClass


class EventSourcingServiceFactory(
    val aggregateRegistry: AggregateRegistry,
    val eventMapper: EventMapper,
    val eventStoreDbOperations: EventStoreDbOperations,
    val eventSourcingProperties: EventSourcingProperties
) {

    inline fun <ID : Any, reified A : Aggregate, S: AggregateState<ID, A>> create(): EventSourcingService<ID, A, S> {
        return EventSourcingService(
            A::class, aggregateRegistry, eventMapper, eventSourcingProperties, eventStoreDbOperations
        )
    }
}