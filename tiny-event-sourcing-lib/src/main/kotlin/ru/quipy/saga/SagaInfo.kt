package ru.quipy.saga

import java.util.*

/**
 * The SagaContext class represents the context of a Saga, containing relevant information about the Saga's execution.
 * It is used to track the state and progression of the Saga.
 *
 * The [ctx] field is an extended version for working with Sagas. It is updated using the [SagaManager].
 * The [correlationId], [currentEventId], and [causationId] fields provide a simplified version for working with Sagas
 * and are always used.
 *
 * @property ctx A map containing information about the Saga,
 * where the key is the Saga name and the value is the corresponding [SagaInfo] instance.
 * @property correlationId The correlation ID for all events in the Saga. It is equivalent to [SagaInfo.sagaInstanceId].
 * @property currentEventId The ID of the current step in the Saga.
 * It is equivalent to [SagaInfo.sagaStepId].
 * @property causationId The causation ID, representing the previous step in the Saga.
 * It is equivalent to [SagaInfo.prevStepsIds].
 */
data class SagaContext(
    val ctx: Map<String, SagaInfo> = mapOf(),
    var correlationId: UUID? = null,
    var currentEventId: UUID? = null,
    var causationId: UUID? = null
)

/**
 * The SagaInfo class represents information about a step in a Saga.
 * @property sagaInstanceId The ID of the Saga instance to which this step belongs.
 * @property stepName The name of the step in the Saga.
 * @property sagaStepId The ID that uniquely identifies this step in the Saga.
 * @property prevStepsIds The set of IDs representing the previous steps in the Saga.
 * @property stepIdPrevStepsIdsAssociation A map associating step IDs with their corresponding previous step IDs.
 * Used when processing parallel steps in [SagaContext.plus].
 */
data class SagaInfo(
    val sagaInstanceId: UUID,
    val stepName: String,
    val sagaStepId: UUID,
    val prevStepsIds: Set<UUID> = setOf(),
    val stepIdPrevStepsIdsAssociation: Map<UUID, Set<UUID>> = mapOf()
)

/**
 * Used when processing a new Saga step in [SagaManager]
 */
data class SagaStep(
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID = UUID.randomUUID(),
    val sagaInstanceId: UUID = UUID.randomUUID(),
    val prevSteps: Set<UUID> = setOf()
)

/**
 * Combines two [SagaContext] objects by merging their `ctx` maps.
 * This method is used to process parallel steps in a Saga.
 *
 * If two [SagaContext] objects have a [SagaInfo] with the same [SagaInfo.sagaInstanceId], they will be merged using the
 * [SagaInfo.stepIdPrevStepsIdsAssociation] field. The remaining SagaInfo objects that exist in at least one SagaContext
 * will be included in the resulting SagaContext.
 *
 * @param other The SagaContext object to be combined with the current object.
 * @return A new SagaContext object with the combined ctx maps.
 * [SagaContext.correlationId], [SagaContext.currentEventId], [SagaContext.causationId]
 * are overwritten from the first SagaContext
 */
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