package ru.quipy.core

import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*
import ru.quipy.mapper.EventMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass


class EventSourcingServiceFactory(
    private val aggregateRegistry: AggregateRegistry,
    private val eventMapper: EventMapper,
    private val eventStoreDbOperations: EventStoreDbOperations,
    private val configProperties: ConfigProperties
) {
    companion object {
        private val logger = LoggerFactory.getLogger(EventSourcingServiceFactory::class.java)
    }

    private val services = ConcurrentHashMap<KClass<*>, EventSourcingService<*>>()

    fun <A : Aggregate> getOrCreateService(aggregateType: KClass<A>): EventSourcingService<A> {
        return services.computeIfAbsent(aggregateType) {
            EventSourcingService(
                aggregateType, aggregateRegistry, eventMapper, configProperties, eventStoreDbOperations
            )
        } as EventSourcingService<A>
    }
}