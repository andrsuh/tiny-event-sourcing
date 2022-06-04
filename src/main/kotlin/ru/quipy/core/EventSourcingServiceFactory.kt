package ru.quipy.core

import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*
import ru.quipy.mapper.EventMapper
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass


class EventSourcingServiceFactory(
    private val aggregateRegistry: AggregateRegistry,
    private val eventMapper: EventMapper,
    private val eventStoreDbOperations: EventStoreDbOperations,
    private val eventSourcingProperties: EventSourcingProperties
) {
    companion object {
        private val logger = LoggerFactory.getLogger(EventSourcingServiceFactory::class.java)
    }

    private val services = ConcurrentHashMap<KClass<*>, EventSourcingService<*>>()

    fun <A : Aggregate> getOrCreateService(aggregateType: KClass<A>): EventSourcingService<A> {
        return services.computeIfAbsent(aggregateType) {
            EventSourcingService(
                aggregateType, aggregateRegistry, eventMapper, eventSourcingProperties, eventStoreDbOperations
            )
        } as EventSourcingService<A>
    }
}