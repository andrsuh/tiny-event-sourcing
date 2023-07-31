package ru.quipy.saga.aggregate.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.UUID

const val SAGA_STEP_LAUNCHED = "SAGA_STEP_LAUNCHED_EVENT"
const val SAGA_STEP_INITIATED = "SAGA_STEP_INITIATED_EVENT"
const val SAGA_STEP_PROCESSED = "SAGA_STEP_PROCESSED_EVENT"
const val DEFAULT_SAGA_PROCESSED = "DEFAULT_SAGA_PROCESSED_EVENT"

@DomainEvent(SAGA_STEP_LAUNCHED)
data class SagaStepLaunchedEvent(
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID,
    val sagaInstanceId: UUID,
    val prevSteps: Set<UUID> = setOf()
) : Event<SagaStepAggregate>(
    name = SAGA_STEP_LAUNCHED,
)

@DomainEvent(SAGA_STEP_INITIATED)
data class SagaStepInitiatedEvent(
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID,
    val sagaInstanceId: UUID,
    val prevSteps: Set<UUID> = setOf()
) : Event<SagaStepAggregate>(
    name = SAGA_STEP_INITIATED,
)

@DomainEvent(SAGA_STEP_PROCESSED)
data class SagaStepProcessedEvent(
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID,
    val sagaInstanceId: UUID,
    val prevSteps: Set<UUID> = setOf(),
    val eventName: String
) : Event<SagaStepAggregate>(
    name = SAGA_STEP_PROCESSED,
)

@DomainEvent(DEFAULT_SAGA_PROCESSED)
data class DefaultSagaProcessedEvent(
    val correlationId: UUID,
    val currentEventId: UUID,
    val causationId: UUID?,
    val eventName: String
) : Event<SagaStepAggregate>(
    name = DEFAULT_SAGA_PROCESSED,
)
