package ru.quipy.saga

import ru.quipy.core.EventSourcingService
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import java.util.*

class SagaManager(
    private val sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>
) {
    private fun launchSaga(sagaName: String, stepName: String, sagaContext: SagaContext): SagaContext {
        if (sagaContext.ctx.containsKey(sagaName))
            throw IllegalArgumentException("The name of the saga $sagaName is already in the context")

        val sagaStep = SagaStep(sagaName, stepName)

        val event = sagaStepEsService.create { it.startSagaStep(sagaStep) }

        return SagaContext(sagaContext.ctx.toMutableMap().also {
            it[sagaName] = SagaInfo(event.sagaInstanceId, event.sagaStepId, event.prevStep)
        })
    }

    fun withContextGiven(sagaContext: SagaContext) = SagaInvoker(sagaContext)
    fun withContextGiven() = SagaInvoker(SagaContext())

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
        fun launchSaga(sagaName: String, stepName: String): SagaInvoker {
            val updatedContext = launchSaga(sagaName, stepName, sagaContext)
            return SagaInvoker(updatedContext)
        }

        fun performSagaStep(sagaName: String, stepName: String): SagaInvoker {
            val updatedContext = performSagaStep(sagaName, stepName, sagaContext)
            return SagaInvoker(updatedContext)
        }
    }
}