package ru.quipy.saga

import java.util.*

data class SagaContext(
    val ctx: Map<String, SagaInfo> = mapOf()
)

data class SagaInfo(
    val sagaInstanceId: UUID,
    val sagaStepId: UUID,
    val prevStepId: UUID?
)

data class SagaStep(
    val sagaName: String,
    val stepName: String,
    val sagaStepId: UUID = UUID.randomUUID(),
    val sagaInstanceId: UUID = UUID.randomUUID(),
    val prevStep: UUID? = null,
)
