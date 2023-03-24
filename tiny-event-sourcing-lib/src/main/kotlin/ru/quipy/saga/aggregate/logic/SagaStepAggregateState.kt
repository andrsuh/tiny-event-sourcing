package ru.quipy.saga.aggregate.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.saga.SagaStep
import ru.quipy.saga.aggregate.api.*
import java.util.UUID

class SagaStepAggregateState: AggregateState<UUID, SagaStepAggregate> {
    private lateinit var sagaName: String
    private lateinit var sagaInstanceId: UUID
    private var sagaSteps = mutableListOf<UUID>()
    override fun getId() = sagaInstanceId

    fun startSagaStep(sagaStep: SagaStep) : SagaStepStartedEvent {
        return SagaStepStartedEvent(
            sagaStep.sagaName,
            sagaStep.stepName,
            sagaStep.sagaStepId,
            sagaStep.sagaInstanceId,
            sagaStep.prevStep
            )
    }

    fun processSagaStep(sagaStep: SagaStep) : SagaStepProcessedEvent {
        return SagaStepProcessedEvent(
            sagaStep.sagaName,
            sagaStep.stepName,
            sagaStep.sagaStepId,
            sagaStep.sagaInstanceId,
            sagaStep.prevStep
            )
    }

    @StateTransitionFunc
    fun startSagaStep(sagaStepEvent: SagaStepStartedEvent) {
        sagaName = sagaStepEvent.sagaName
        sagaInstanceId = sagaStepEvent.sagaInstanceId
        sagaSteps.add(sagaStepEvent.sagaStepId)
    }

    @StateTransitionFunc
    fun processSagaStep(sagaStepEvent: SagaStepProcessedEvent) {
        sagaSteps.add(sagaStepEvent.sagaStepId)
    }
}