package ru.quipy.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.*
import ru.quipy.database.EventStore
import ru.quipy.mapper.EventMapper
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.saga.SagaManager
import ru.quipy.saga.aggregate.api.*
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import ru.quipy.saga.aggregate.stream.SagaEventStream
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.EventStoreStreamReaderManager
import ru.quipy.streams.EventStreamReaderManager
import java.util.*

@Configuration
class EventSourcingLibConfig {
    @Bean
    @ConditionalOnMissingBean
    fun jsonObjectMapper() = jacksonObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)

    @Bean
    @ConditionalOnMissingBean
    fun eventMapper(jsonObjectMapper: ObjectMapper): EventMapper = JsonEventMapper(jsonObjectMapper)

    @Bean
    @ConfigurationProperties(prefix = "event.sourcing")
    @ConditionalOnMissingBean
    fun configProperties() = EventSourcingProperties()

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    fun aggregateRegistry(
        eventSourcingProperties: EventSourcingProperties
    ): SeekingForSuitableClassesAggregateRegistry {
        val aggregateRegistry = SeekingForSuitableClassesAggregateRegistry(
            BasicAggregateRegistry(),
            eventSourcingProperties
        )
        aggregateRegistry.register(SagaStepAggregate::class, SagaStepAggregateState::class) {
            registerStateTransition(SagaStepLaunchedEvent::class, SagaStepAggregateState::launchSagaStep)
            registerStateTransition(SagaStepInitiatedEvent::class, SagaStepAggregateState::initiateSagaStep)
            registerStateTransition(SagaStepProcessedEvent::class, SagaStepAggregateState::processSagaStep)
            registerStateTransition(DefaultSagaProcessedEvent::class, SagaStepAggregateState::processDefaultSaga)
        }
        return aggregateRegistry
    }


    @Bean
    @ConditionalOnBean(EventStore::class)
    @ConditionalOnMissingBean(EventStreamReaderManager::class)
    fun eventStreamReaderManager(
        eventSourcingProperties: EventSourcingProperties,
        eventStore: EventStore
    ) = EventStoreStreamReaderManager(eventStore, eventSourcingProperties)

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(EventStore::class)
    @ConditionalOnMissingBean
    fun eventStreamManager(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventStore: EventStore,
        eventStreamReaderManager: EventStreamReaderManager
    ) = AggregateEventStreamManager(
        aggregateRegistry,
        eventStore,
        eventSourcingProperties,
        eventStreamReaderManager
    )

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(EventStore::class)
    @ConditionalOnMissingBean
    fun subscriptionManager(
        eventStreamManager: AggregateEventStreamManager,
        aggregateRegistry: AggregateRegistry,
        eventMapper: EventMapper
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
        eventMapper: EventMapper,
        eventStore: EventStore
    ) = EventSourcingServiceFactory(
        aggregateRegistry, eventMapper, eventStore, eventSourcingProperties
    )

    @Bean
    @ConditionalOnBean(EventStore::class)
    @ConditionalOnMissingBean(name = ["sagaStepEsService"])
    @ConditionalOnProperty(
        prefix = "event.sourcing",
        name = ["sagas-enabled"],
        havingValue = "true",
        matchIfMissing = true
    )
    fun sagaStepEsService(
        aggregateRegistry: AggregateRegistry,
        eventMapper: EventMapper,
        configProperties: EventSourcingProperties,
        eventStore: EventStore
    ) = EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>(
        SagaStepAggregate::class,
        aggregateRegistry,
        eventMapper,
        configProperties,
        eventStore
    )

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = ["sagaStepEsService"])
    fun sagaManager(
        sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>
    ) = SagaManager(sagaStepEsService)

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = ["sagaStepEsService"])
    fun sagaEventStream(
        aggregateRegistry: AggregateRegistry,
        eventStreamManager: AggregateEventStreamManager,
        sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>,
        configProperties: EventSourcingProperties,
    ) = SagaEventStream(aggregateRegistry, eventStreamManager, sagaStepEsService, configProperties)
}