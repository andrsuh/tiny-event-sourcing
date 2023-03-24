package ru.quipy.saga

import ru.quipy.core.EventSourcingService
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.logic.SagaStep
import java.util.*

class SagaManager(
    private val sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStep>
) {
    fun launchSaga(sagaName: String, stepName: String): ru.quipy.saga.SagaStep {
        val sagaStep = SagaStep(sagaName, stepName)
        sagaStepEsService.create { it.startSagaStep(sagaStep) }
        return sagaStep
    }

    fun performSagaStep(sagaName: String, stepName: String, sagaContext: SagaContext): ru.quipy.saga.SagaStep {
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