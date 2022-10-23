package ru.quipy.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.*
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager

@Configuration
class EventSourcingLibConfig {
    @Bean
    fun jsonObjectMapper() = jacksonObjectMapper()

    @Bean//@ConditionalOnMissingBean(EventMapper::class)
    fun eventMapper(jsonObjectMapper: ObjectMapper) = JsonEventMapper(jsonObjectMapper)

    @Bean
    @ConfigurationProperties(prefix = "event.sourcing")
    fun configProperties() = EventSourcingProperties()

    @Bean//@ConditionalOnBean(MongoTemplate::class)
    fun eventStoreDbOperations() = MongoDbEventStoreDbOperations()

    @Bean(initMethod = "init")
    fun aggregateRegistry(eventSourcingProperties: EventSourcingProperties) =
        SeekingForSuitableClassesAggregateRegistry(BasicAggregateRegistry(), eventSourcingProperties)

    @Bean(destroyMethod = "destroy")
    fun eventStreamManager(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventStoreDbOperations: EventStoreDbOperations
    ) = AggregateEventStreamManager(
        aggregateRegistry,
        eventStoreDbOperations,
        eventSourcingProperties
    )

    @Bean(destroyMethod = "destroy")
    fun subscriptionManager(
        eventStreamManager: AggregateEventStreamManager,
        aggregateRegistry: AggregateRegistry,
        eventMapper: JsonEventMapper,
    ) = AggregateSubscriptionsManager(
        eventStreamManager,
        aggregateRegistry,
        eventMapper
    )

    @Bean
    fun eventSourcingServiceFactory(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventMapper: JsonEventMapper,
        eventStoreDbOperations: EventStoreDbOperations
    ) = EventSourcingServiceFactory(
        aggregateRegistry, eventMapper, eventStoreDbOperations, eventSourcingProperties
    )
}