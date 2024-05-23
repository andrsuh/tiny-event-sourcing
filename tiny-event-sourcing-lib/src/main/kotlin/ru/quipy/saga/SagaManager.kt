package ru.quipy.saga

import ru.quipy.core.EventSourcingService
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import java.util.*

class SagaManager(
    private val sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>
) {
    fun withContextGiven(sagaContext: SagaContext?) = SagaInvoker(sagaContext)

    fun launchSaga(sagaName: String, stepName: String) =
        SagaInvoker(SagaContext()).launchSaga(sagaName, stepName)

    private fun launchSaga(
        sagaName: String,
        stepName: String,
        sagaStepId: UUID?,
        sagaContext: SagaContext
    ): SagaContext {
        if (sagaContext.ctx.containsKey(sagaName))
            throw IllegalArgumentException("The name of the saga $sagaName is already in the context")

        val sagaStep = SagaStep(sagaName, stepName, sagaStepId ?: UUID.randomUUID())

        sagaStepEsService.create { it.launchSagaStep(sagaStep) }

        return processContext(sagaContext, sagaName, sagaStep)
    }

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

        sagaStepEsService.update(sagaStep.sagaInstanceId) { it.initiateSagaStep(sagaStep) }

        return processContext(sagaContext, sagaName, sagaStep)
    }

    private fun processContext(sagaContext: SagaContext, sagaName: String, sagaStep: SagaStep): SagaContext {
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

        return processedContext
    }

    inner class SagaInvoker(
        private val sagaContext: SagaContext?,
        private var currentSagaStepId: UUID? = null
    ) {
        fun launchSaga(sagaName: String, stepName: String): SagaInvoker {
            val updatedContext = launchSaga(sagaName, stepName, currentSagaStepId, sagaContext ?: SagaContext())
            currentSagaStepId = updatedContext.ctx[sagaName]!!.sagaStepId
            return SagaInvoker(updatedContext, currentSagaStepId)
        }

        fun performSagaStep(sagaName: String, stepName: String): SagaInvoker {
            if (sagaContext == null)
                throw IllegalArgumentException("The saga context is not initialized")

            val updatedContext = performSagaStep(sagaName, stepName, currentSagaStepId, sagaContext)
            currentSagaStepId = updatedContext.ctx[sagaName]!!.sagaStepId
            return SagaInvoker(updatedContext, currentSagaStepId)
        }

        fun sagaContext() = sagaContext ?: SagaContext()
    }
}