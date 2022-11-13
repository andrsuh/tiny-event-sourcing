package ru.quipy.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import ru.quipy.core.*
import ru.quipy.database.EventStore
import ru.quipy.eventstore.MongoClientEventStore
import ru.quipy.eventstore.converter.JacksonMongoEntityConverter
import ru.quipy.eventstore.factory.MongoClientFactory
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


    @Bean
    @ConditionalOnBean(MongoTemplate::class)
    fun mongoTemplateEventStore() : EventStore = MongoTemplateEventStore()

    @Bean
    @Primary
    @ConditionalOnBean(MongoClientFactory::class)
    fun mongoClientEventStore(databaseFactory : MongoClientFactory) : EventStore {
        return MongoClientEventStore(JacksonMongoEntityConverter(), databaseFactory)
    }
    @Bean
    @ConditionalOnBean(MongoTransactionManager::class)
    fun transactionManager(dbFactory: MongoDatabaseFactory): MongoTransactionManager {
        return MongoTransactionManager(dbFactory)
    }

    @Bean(initMethod = "init")
    fun aggregateRegistry(eventSourcingProperties: EventSourcingProperties) =
        SeekingForSuitableClassesAggregateRegistry(BasicAggregateRegistry(), eventSourcingProperties)

    @Bean(destroyMethod = "destroy")
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
        eventStore: EventStore
    ) = EventSourcingServiceFactory(
        aggregateRegistry, eventMapper, eventStore, eventSourcingProperties
    )
}