package ru.quipy.saga.aggregate.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.UUID

const val SAGA_STEP_STARTED = "SAGA_STEP_STARTED_EVENT"
const val SAGA_STEP_PROCESSED = "SAGA_STEP_PROCESSED_EVENT"

@DomainEvent(SAGA_STEP_STARTED)
data class SagaStepStartedEvent (
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID,
    val sagaInstanceId: UUID,
    val prevSteps: Set<UUID> = setOf()
) : Event<SagaStepAggregate>(
    name = SAGA_STEP_STARTED,
)

@DomainEvent(SAGA_STEP_PROCESSED)
data class SagaStepProcessedEvent (
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID,
    val sagaInstanceId: UUID,
    val prevSteps: Set<UUID> = setOf()
) : Event<SagaStepAggregate>(
    name = SAGA_STEP_PROCESSED,
)
