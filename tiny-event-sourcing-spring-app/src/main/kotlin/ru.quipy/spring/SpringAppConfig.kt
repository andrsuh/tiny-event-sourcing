package ru.quipy.spring

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.streams.AggregateEventsStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager

@Configuration
open class SpringAppConfig {
    @Bean
    fun jsonObjectMapper() = jacksonObjectMapper()

    @Bean
    //@ConditionalOnMissingBean(EventMapper::class)
    fun eventMapper(jsonObjectMapper: ObjectMapper) = JsonEventMapper(jsonObjectMapper)

    @Bean
    @ConfigurationProperties(prefix = "event.sourcing")
    fun configProperties() = EventSourcingProperties()

    @Bean
    //@ConditionalOnBean(MongoTemplate::class)
    fun eventStoreDbOperations() = MongoDbEventStoreDbOperations()

    @Bean
    fun aggregateRegistry() = AggregateRegistry()

    @Bean(destroyMethod = "destroy")
    fun eventStreamManager(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventMapper: JsonEventMapper,
        eventStoreDbOperations: EventStoreDbOperations
    ) = AggregateEventsStreamManager(
        aggregateRegistry,
        eventStoreDbOperations,
        eventMapper,
        eventSourcingProperties
    )

    @Bean(destroyMethod = "destroy")
    fun subscriptionManager(eventStreamManager: AggregateEventsStreamManager) =
        AggregateSubscriptionsManager(eventStreamManager)

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