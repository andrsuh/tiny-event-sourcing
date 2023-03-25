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

    fun withContextGiven(sagaContext: SagaContext) = SagaInvoker(sagaContext)

    private fun performSagaStep(sagaName: String, stepName: String, sagaContext: SagaContext): SagaContext {
        if (!sagaContext.ctx.containsKey(sagaName))
            throw IllegalArgumentException("The name of the saga $sagaName does not match the context")

        val sagaInfo = sagaContext.ctx[sagaName]

        val sagaStep = SagaStep(
            sagaName,
            stepName,
            sagaInstanceId = sagaInfo!!.sagaInstanceId,
            prevStep = sagaInfo.sagaStepId
        )
        val event = sagaStepEsService.update(sagaStep.sagaInstanceId) { it.processSagaStep(sagaStep) }

        return SagaContext(sagaContext.ctx.toMutableMap().also {
            it[sagaName] = SagaInfo(event.sagaInstanceId, event.sagaStepId, event.prevStep)
        })
    }

    inner class SagaInvoker(
        val sagaContext: SagaContext
    ) {
        fun performSagaStep(sagaName: String, stepName: String): SagaInvoker {
            val updatedContext = performSagaStep(sagaName, stepName, sagaContext)
            return SagaInvoker(updatedContext)
        }
    }
}