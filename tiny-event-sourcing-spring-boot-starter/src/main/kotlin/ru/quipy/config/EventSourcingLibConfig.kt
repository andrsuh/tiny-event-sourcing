package ru.quipy.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.*
import ru.quipy.database.EventStore
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager

@Configuration
class EventSourcingLibConfig {
    @Bean
    @ConditionalOnMissingBean
    fun jsonObjectMapper() = jacksonObjectMapper()

    @Bean
    @ConditionalOnMissingBean
    fun eventMapper(jsonObjectMapper: ObjectMapper) = JsonEventMapper(jsonObjectMapper)

    @Bean
    @ConfigurationProperties(prefix = "event.sourcing")
    @ConditionalOnMissingBean
    fun configProperties() = EventSourcingProperties()

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    fun aggregateRegistry(eventSourcingProperties: EventSourcingProperties) =
        SeekingForSuitableClassesAggregateRegistry(BasicAggregateRegistry(), eventSourcingProperties)

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(EventStore::class)
    @ConditionalOnMissingBean
    fun eventStreamManager(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventStore: EventStore
    ) = AggregateEventStreamManager(
        aggregateRegistry,
        eventStore,
        eventSourcingProperties
    )

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(EventStore::class)
    @ConditionalOnMissingBean
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
    @ConditionalOnBean(EventStore::class)
    @ConditionalOnMissingBean
    fun eventSourcingServiceFactory(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventMapper: JsonEventMapper,
        eventStore: EventStore
    ) = EventSourcingServiceFactory(
        aggregateRegistry, eventMapper, eventStore, eventSourcingProperties
    )
}