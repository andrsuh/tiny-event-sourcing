package ru.quipy.saga

import ru.quipy.core.EventSourcingService
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import java.util.*

class SagaManager(
    private val sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>
) {
    fun launchSaga(sagaName: String, stepName: String): SagaStep {
        val sagaStep = SagaStep(sagaName, stepName)
        sagaStepEsService.create { it.startSagaStep(sagaStep) }
        return sagaStep
    }

    fun performSagaStep(sagaName: String, stepName: String, sagaContext: SagaContext): SagaStep {
        if (!sagaContext.ctx.containsKey(sagaName))
            throw IllegalArgumentException("The name of the saga $sagaName does not match the context")

        val prevSagaInfo = sagaContext.ctx[sagaName]
        val sagaStep = SagaStep(
            sagaName,
            stepName,
            sagaInstanceId = prevSagaInfo!!.sagaInstanceId,
            prevStep = prevSagaInfo.sagaStepId
        )

        sagaStepEsService.update(sagaStep.sagaInstanceId) { it.processSagaStep(sagaStep) }
        return sagaStep
    }
}