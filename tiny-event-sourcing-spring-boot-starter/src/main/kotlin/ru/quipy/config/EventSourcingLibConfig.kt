package ru.quipy.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import org.bson.UuidRepresentation
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import ru.quipy.core.*
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.eventstore.JacksonMongoEntityConverter
import ru.quipy.eventstore.MongoClientDbEventStoreDbOperations
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
    //@ConditionalOnBean(MongoTemplate::class)
    fun eventStoreDbOperations() : EventStoreDbOperations {
//        return MongoDbEventStoreDbOperations()
        val clientSettings: MongoClientSettings =
            MongoClientSettings.builder()
                .applyConnectionString(ConnectionString("mongodb://localhost:27017"))
                .uuidRepresentation(UuidRepresentation.STANDARD).build()
        val mongoClient = MongoClients.create(clientSettings)
        return MongoClientDbEventStoreDbOperations(
            mongoClient,
            "tiny-es",
            JacksonMongoEntityConverter()
        )
    }

    @Bean
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