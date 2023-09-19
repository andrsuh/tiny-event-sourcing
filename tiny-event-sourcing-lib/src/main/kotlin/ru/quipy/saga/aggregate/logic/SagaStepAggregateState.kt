package ru.quipy.saga.aggregate.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.saga.SagaStep
import ru.quipy.saga.aggregate.api.*
import java.util.UUID

class SagaStepAggregateState : AggregateState<UUID, SagaStepAggregate> {
    private lateinit var sagaName: String
    private lateinit var sagaInstanceId: UUID
    private var sagaSteps = mutableListOf<UUID>()
    private var processedSagaSteps = mutableListOf<UUID>()
    override fun getId() = sagaInstanceId

    fun launchSagaStep(sagaStep: SagaStep): SagaStepLaunchedEvent {
        return SagaStepLaunchedEvent(
            sagaStep.sagaName,
            sagaStep.stepName,
            sagaStep.sagaStepId,
            sagaStep.sagaInstanceId,
            sagaStep.prevSteps
        )
    }

    fun initiateSagaStep(sagaStep: SagaStep): SagaStepInitiatedEvent {
        if (sagaSteps.contains(sagaStep.sagaStepId)) {
            throw IllegalStateException("Duplicate step: $sagaStep")
        }

        return SagaStepInitiatedEvent(
            sagaStep.sagaName,
            sagaStep.stepName,
            sagaStep.sagaStepId,
            sagaStep.sagaInstanceId,
            sagaStep.prevSteps
        )
    }

    fun processSagaStep(sagaStep: SagaStep, eventName: String): SagaStepProcessedEvent {
        if (processedSagaSteps.contains(sagaStep.sagaStepId)) {
            throw IllegalStateException("Duplicate step: $sagaStep")
        }

        return SagaStepProcessedEvent(
            sagaStep.sagaName,
            sagaStep.stepName,
            sagaStep.sagaStepId,
            sagaStep.sagaInstanceId,
            sagaStep.prevSteps,
            eventName
        )
    }

    fun containsProcessedSagaStep(sagaStepId: UUID): Boolean {
        return processedSagaSteps.contains(sagaStepId)
    }

    fun processDefaultSaga(
        correlationId: UUID,
        currentEventId: UUID,
        eventName: String,
        causationId: UUID? = null
    ): DefaultSagaProcessedEvent {
        if (processedSagaSteps.contains(currentEventId)) {
            throw IllegalStateException("Duplicate step: $correlationId")
        }

        return DefaultSagaProcessedEvent(
            correlationId,
            currentEventId,
            causationId,
            eventName
        )
    }

    @StateTransitionFunc
    fun launchSagaStep(sagaStepEvent: SagaStepLaunchedEvent) {
        sagaName = sagaStepEvent.sagaName
        sagaInstanceId = sagaStepEvent.sagaInstanceId
        sagaSteps.add(sagaStepEvent.sagaStepId)
    }

    @StateTransitionFunc
    fun initiateSagaStep(sagaStepEvent: SagaStepInitiatedEvent) {
        sagaSteps.add(sagaStepEvent.sagaStepId)
    }

    @StateTransitionFunc
    fun processSagaStep(sagaStepEvent: SagaStepProcessedEvent) {
        processedSagaSteps.add(sagaStepEvent.sagaStepId)
    }

    @StateTransitionFunc
    fun processDefaultSaga(sagaEvent: DefaultSagaProcessedEvent) {
        if (!this::sagaInstanceId.isInitialized) {
            sagaInstanceId = sagaEvent.correlationId
        }
        processedSagaSteps.add(sagaEvent.currentEventId)
    }
}