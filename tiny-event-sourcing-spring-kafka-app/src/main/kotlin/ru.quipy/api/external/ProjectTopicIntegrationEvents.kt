package ru.quipy.api.external

import ru.quipy.core.annotations.IntegrationEvent
import ru.quipy.domain.ExternalEvent

const val PROJECT_CREATED_EXTERNAL_EVENT = "PROJECT_CREATED_EVENT"
const val TAG_CREATED_EXTERNAL_EVENT = "TAG_CREATED_EVENT"
const val TAG_ASSIGNED_TO_TASK_EXTERNAL_EVENT = "TAG_ASSIGNED_TO_TASK_EVENT"
const val TASK_CREATED_EXTERNAL_EVENT = "TASK_CREATED_EVENT"

@IntegrationEvent(name = PROJECT_CREATED_EXTERNAL_EVENT)
class ProjectCreatedExternalEvent(
    val title: String,
    val creatorId: String,
    createdAt: Long = System.currentTimeMillis(),
) : ExternalEvent<ProjectTopic>(
    name = PROJECT_CREATED_EXTERNAL_EVENT,
    createdAt = createdAt,
)

@IntegrationEvent(name = TASK_CREATED_EXTERNAL_EVENT)
class TaskAndTagCreatedExternalEvent(
    val taskName: String,
    val tagName: String,
    createdAt: Long = System.currentTimeMillis(),
) : ExternalEvent<ProjectTopic>(
    name = TASK_CREATED_EXTERNAL_EVENT,
    createdAt = createdAt
)