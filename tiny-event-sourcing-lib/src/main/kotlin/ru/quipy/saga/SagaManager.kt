package ru.quipy.saga

import ru.quipy.core.EventSourcingService
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import java.util.*

class SagaManager(
    private val sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>
) {
    private fun launchSaga(
        sagaName: String,
        stepName: String,
        sagaStepId: UUID?,
        sagaContext: SagaContext
    ): SagaContext {
        if (sagaContext.ctx.containsKey(sagaName))
            throw IllegalArgumentException("The name of the saga $sagaName is already in the context")

        val sagaStep = SagaStep(sagaName, stepName, sagaStepId ?: UUID.randomUUID())

        val processedContext = SagaContext(sagaContext.ctx.toMutableMap().also {
            it[sagaName] = SagaInfo(
                sagaStep.sagaInstanceId,
                sagaStep.stepName,
                sagaStep.sagaStepId,
                sagaStep.prevSteps,
                mapOf(sagaStep.sagaStepId to sagaStep.prevSteps)
            )
        })

        sagaStepEsService.create { it.launchSagaStep(sagaStep) }

        return processedContext
    }

    fun withContextGiven(sagaContext: SagaContext) = SagaInvoker(sagaContext)
    fun launchSaga(sagaName: String, stepName: String) =
        SagaInvoker(SagaContext()).launchSaga(sagaName, stepName)

    private fun performSagaStep(
        sagaName: String,
        stepName: String,
        sagaStepId: UUID?,
        sagaContext: SagaContext,
    ): SagaContext {
        if (!sagaContext.ctx.containsKey(sagaName))
            throw IllegalArgumentException("The name of the saga $sagaName does not match the context")

        val sagaInfo = sagaContext.ctx[sagaName]

        val sagaStep = SagaStep(
            sagaName,
            stepName,
            sagaStepId ?: UUID.randomUUID(),
            sagaInstanceId = sagaInfo!!.sagaInstanceId,
            prevSteps = sagaInfo.stepIdPrevStepsIdsAssociation.keys
        )

        val processedContext = SagaContext(sagaContext.ctx.toMutableMap().also {
            it[sagaName] = SagaInfo(
                sagaStep.sagaInstanceId,
                sagaStep.stepName,
                sagaStep.sagaStepId,
                sagaStep.prevSteps,
                mapOf(sagaStep.sagaStepId to sagaStep.prevSteps)
            )
        })
        processedContext.correlationId = sagaContext.correlationId
        processedContext.causationId = sagaContext.causationId
        processedContext.currentEventId = sagaContext.currentEventId

        sagaStepEsService.update(sagaStep.sagaInstanceId) { it.initiateSagaStep(sagaStep) }

        return processedContext
    }

    inner class SagaInvoker(
        val sagaContext: SagaContext,
        private var currentSagaStepId: UUID? = null
    ) {
        fun launchSaga(sagaName: String, stepName: String): SagaInvoker {
            val updatedContext = launchSaga(sagaName, stepName, currentSagaStepId, sagaContext)
            currentSagaStepId = updatedContext.ctx[sagaName]!!.sagaStepId
            return SagaInvoker(updatedContext, currentSagaStepId)
        }

        fun performSagaStep(sagaName: String, stepName: String): SagaInvoker {
            val updatedContext = performSagaStep(sagaName, stepName, currentSagaStepId, sagaContext)
            currentSagaStepId = updatedContext.ctx[sagaName]!!.sagaStepId
            return SagaInvoker(updatedContext, currentSagaStepId)
        }
    }
}