package ru.quipy

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import ru.quipy.converter.EntityConverter
import ru.quipy.converter.JsonEntityConverter
import ru.quipy.converter.ResultSetToEntityMapper
import ru.quipy.converter.ResultSetToEntityMapperImpl
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.BasicAggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.core.SeekingForSuitableClassesAggregateRegistry
import ru.quipy.database.EventStore
import ru.quipy.db.DataSourceProvider
import ru.quipy.db.DatasourceProviderImpl
import ru.quipy.db.factory.ConnectionFactory
import ru.quipy.db.factory.DataSourceConnectionFactoryImpl
import ru.quipy.executor.ExceptionLoggingSqlQueriesExecutor
import ru.quipy.executor.QueryExecutor
import ru.quipy.mapper.EventMapper
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
import ru.quipy.streams.EventStreamReaderManager
import java.util.Properties
import java.util.UUID
import javax.sql.DataSource

class TinyEsLibConfig private constructor() {
    lateinit var objectMapper: ObjectMapper
    lateinit var eventMapper: EventMapper
    lateinit var eventSourcingProperties: EventSourcingProperties
    lateinit var aggregateRegistry: AggregateRegistry
    lateinit var eventStreamReaderManager: EventStreamReaderManager
    lateinit var eventStreamManager: AggregateEventStreamManager
    lateinit var subscriptionsManager: AggregateSubscriptionsManager
    lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory
    lateinit var sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>
    lateinit var sagaManager: SagaManager
    lateinit var sagaEventStream: SagaEventStream
    lateinit var entityConverter: EntityConverter
    lateinit var resultSetToEntityMapper: ResultSetToEntityMapper
    lateinit var datasourceProvider: DataSourceProvider
    lateinit var executor: QueryExecutor
    lateinit var eventStore: EventStore
    lateinit var databaseConnectionFactory: ConnectionFactory
    lateinit var schema: String
    constructor(properties: Properties, dataSource: DataSource) : this() {
        this.objectMapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        this.eventMapper = JsonEventMapper(this.objectMapper)
        this.eventSourcingProperties = EventSourcingProperties()

        this.aggregateRegistry = SeekingForSuitableClassesAggregateRegistry(
            BasicAggregateRegistry(),
            eventSourcingProperties
        )

        aggregateRegistry.register(SagaStepAggregate::class, SagaStepAggregateState::class) {
            registerStateTransition(SagaStepLaunchedEvent::class, SagaStepAggregateState::launchSagaStep)
            registerStateTransition(SagaStepInitiatedEvent::class, SagaStepAggregateState::initiateSagaStep)
            registerStateTransition(SagaStepProcessedEvent::class, SagaStepAggregateState::processSagaStep)
            registerStateTransition(DefaultSagaProcessedEvent::class, SagaStepAggregateState::processDefaultSaga)
        }

        this.schema = properties.getProperty("tiny-es.storage.schema")
        this.entityConverter = JsonEntityConverter(objectMapper)
        this.resultSetToEntityMapper = ResultSetToEntityMapperImpl(entityConverter)
        this.datasourceProvider = DatasourceProviderImpl(dataSource)
        this.databaseConnectionFactory = DataSourceConnectionFactoryImpl(datasourceProvider)
        this.executor = ExceptionLoggingSqlQueriesExecutor(databaseConnectionFactory, PostgresClientEventStore.logger)

        this.eventStore = PostgresClientEventStore(schema, resultSetToEntityMapper, entityConverter, executor)
        this.eventStreamReaderManager = EventStoreStreamReaderManager(eventStore, eventSourcingProperties)
        this.eventStreamManager = AggregateEventStreamManager(
            aggregateRegistry,
            eventStore,
            eventSourcingProperties,
            eventStreamReaderManager
        )
        this.subscriptionsManager = AggregateSubscriptionsManager(
            eventStreamManager,
            aggregateRegistry,
            eventMapper
        )
        this.eventSourcingServiceFactory = EventSourcingServiceFactory(
            aggregateRegistry, eventMapper, eventStore, eventSourcingProperties
        )
        this.sagaStepEsService = EventSourcingService(
            SagaStepAggregate::class,
            aggregateRegistry,
            eventMapper,
            eventSourcingProperties,
            eventStore
        )
        this.sagaManager = SagaManager(sagaStepEsService)
        this.sagaEventStream = SagaEventStream(aggregateRegistry, eventStreamManager, sagaStepEsService)
    }
}