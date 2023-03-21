package ru.quipy.saga

object SagaManager {
    fun launchSaga(sagaName: String, stepName: String): SagaStep {
        return SagaStep(sagaName, stepName)
    }

    fun performSagaStep(sagaName: String, stepName: String, sagaContext: SagaContext): SagaStep {
        if (!sagaContext.ctx.containsKey(sagaName))
            throw IllegalArgumentException("The name of the saga $sagaName does not match the context")

        val prevContext = sagaContext.ctx[sagaName]

        return SagaStep(
            sagaName,
            stepName,
            sagaInstanceId = prevContext!!.sagaInstanceId,
            prevStep = prevContext.sagaStepId
        )
    }
}