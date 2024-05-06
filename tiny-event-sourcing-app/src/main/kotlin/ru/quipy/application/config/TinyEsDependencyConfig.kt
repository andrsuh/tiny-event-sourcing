package ru.quipy.application.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import ru.quipy.PostgresClientEventStore
import ru.quipy.TinyEsLibConfig
import ru.quipy.converter.JsonEntityConverter
import ru.quipy.converter.ResultSetToEntityMapperImpl
import ru.quipy.core.BasicAggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.core.SeekingForSuitableClassesAggregateRegistry
import ru.quipy.db.DatasourceProviderImpl
import ru.quipy.db.factory.DataSourceConnectionFactoryImpl
import ru.quipy.executor.ExceptionLoggingSqlQueriesExecutor
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.saga.SagaManager
import ru.quipy.saga.aggregate.api.DefaultSagaProcessedEvent
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.api.SagaStepInitiatedEvent
import ru.quipy.saga.aggregate.api.SagaStepLaunchedEvent
import ru.quipy.saga.aggregate.api.SagaStepProcessedEvent
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import ru.quipy.saga.aggregate.stream.SagaEventStream
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.EventStoreStreamReaderManager
import java.util.Properties
import java.util.UUID
import javax.sql.DataSource

class TinyEsDependencyConfig private constructor() {
    lateinit var tinyEsLibConfig: TinyEsLibConfig
    constructor(properties: Properties, dataSource: DataSource, eventSourcingProperties: EventSourcingProperties) : this() {
        val objectMapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        val eventMapper = JsonEventMapper(objectMapper)

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
        aggregateRegistry.init()

        val schema = properties.getProperty("tiny-es.storage.schema")
        val entityConverter = JsonEntityConverter(objectMapper)
        val resultSetToEntityMapper = ResultSetToEntityMapperImpl(entityConverter)
        val datasourceProvider = DatasourceProviderImpl(dataSource)
        val databaseConnectionFactory = DataSourceConnectionFactoryImpl(datasourceProvider)
        val executor = ExceptionLoggingSqlQueriesExecutor(databaseConnectionFactory, PostgresClientEventStore.logger)

        val eventStore = PostgresClientEventStore(schema, resultSetToEntityMapper, entityConverter, executor)
        val eventStreamReaderManager = EventStoreStreamReaderManager(eventStore, eventSourcingProperties)
        val eventStreamManager = AggregateEventStreamManager(
            aggregateRegistry,
            eventStore,
            eventSourcingProperties,
            eventStreamReaderManager
        )
        val subscriptionsManager = AggregateSubscriptionsManager(
            eventStreamManager,
            aggregateRegistry,
            eventMapper
        )
        val eventSourcingServiceFactory = EventSourcingServiceFactory(
            aggregateRegistry, eventMapper, eventStore, eventSourcingProperties
        )
        val sagaStepEsService = EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>(
            SagaStepAggregate::class,
            aggregateRegistry,
            eventMapper,
            eventSourcingProperties,
            eventStore
        )
        val sagaManager = SagaManager(sagaStepEsService)
        val sagaEventStream = SagaEventStream(aggregateRegistry, eventStreamManager, sagaStepEsService)

        this.tinyEsLibConfig = TinyEsLibConfig(
            objectMapper,
            eventMapper,
            eventSourcingProperties,
            aggregateRegistry,
            eventStreamReaderManager,
            eventStreamManager,
            subscriptionsManager,
            eventSourcingServiceFactory,
            sagaStepEsService,
            sagaManager,
            sagaEventStream,
            entityConverter,
            resultSetToEntityMapper,
            datasourceProvider,
            eventStore
        )
    }
}