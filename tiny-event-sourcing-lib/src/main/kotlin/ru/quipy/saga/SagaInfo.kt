package ru.quipy.saga

import java.util.*

data class SagaContext(
    val ctx: Map<String, SagaInfo> = mapOf(),
    var correlationId: UUID? = null,
    var currentEventId: UUID? = null,
    var causationId: UUID? = null
)

data class SagaInfo(
    val sagaInstanceId: UUID,
    val stepName: String,
    val sagaStepId: UUID,
    val prevStepsIds: Set<UUID> = setOf(),
    val stepIdPrevStepsIdsAssociation: Map<UUID, Set<UUID>> = mapOf()
)

data class SagaStep(
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID = UUID.randomUUID(),
    val sagaInstanceId: UUID = UUID.randomUUID(),
    val prevSteps: Set<UUID> = setOf()
)

operator fun SagaContext.plus(other: SagaContext): SagaContext {
    val combinedCtx = this.ctx.toMutableMap()

    for ((key, value) in other.ctx) {
        val existingInfo = combinedCtx[key] ?: value.copy()

        if (existingInfo.sagaInstanceId == value.sagaInstanceId) {
            val combinedStepId = existingInfo.stepIdPrevStepsIdsAssociation.toMutableMap()
            combinedStepId.putAll(value.stepIdPrevStepsIdsAssociation)
            combinedCtx[key] = existingInfo.copy(stepIdPrevStepsIdsAssociation = combinedStepId)
        } else {
            combinedCtx[key] = value.copy()
        }
    }

    return SagaContext(combinedCtx, correlationId, currentEventId, causationId)
}