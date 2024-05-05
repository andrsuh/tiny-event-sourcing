package ru.quipy

import com.fasterxml.jackson.databind.ObjectMapper
import ru.quipy.converter.EntityConverter
import ru.quipy.converter.ResultSetToEntityMapper
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.database.EventStore
import ru.quipy.db.DataSourceProvider
import ru.quipy.mapper.EventMapper
import ru.quipy.saga.SagaManager
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import ru.quipy.saga.aggregate.stream.SagaEventStream
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.EventStreamReaderManager
import java.util.UUID

class TinyEsLibConfig (
    val objectMapper: ObjectMapper,
    val eventMapper: EventMapper,
    val eventSourcingProperties: EventSourcingProperties,
    val aggregateRegistry: AggregateRegistry,
    val eventStreamReaderManager: EventStreamReaderManager,
    val eventStreamManager: AggregateEventStreamManager,
    val subscriptionsManager: AggregateSubscriptionsManager,
    val eventSourcingServiceFactory: EventSourcingServiceFactory,
    val sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>,
    val sagaManager: SagaManager,
    val sagaEventStream: SagaEventStream,
    val entityConverter: EntityConverter,
    val resultSetToEntityMapper: ResultSetToEntityMapper,
    val datasourceProvider: DataSourceProvider,
    val eventStore: EventStore,
    // val databaseConnectionFactory: ConnectionFactory
) {}