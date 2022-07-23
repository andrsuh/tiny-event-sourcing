package ru.quipy.demo

import ru.quipy.core.DomainEvent
import ru.quipy.domain.Event
import java.util.*

const val TAG_CREATED_EVENT = "TAG_CREATED_EVENT"
const val TAG_ASSIGNED_TO_TASK_EVENT = "TAG_ASSIGNED_TO_TASK_EVENT"
const val TASK_CREATED_EVENT = "TASK_CREATED_EVENT"

@DomainEvent(name = TAG_CREATED_EVENT)
class TagCreatedEvent(
    val projectId: String,
    val tagId: UUID,
    val tagName: String,
    createdAt: Long = System.currentTimeMillis(),
) : Event<ProjectAggregate>(
    name = TAG_CREATED_EVENT,
    aggregateId = projectId,
    createdAt = createdAt,
) {
    override fun applyTo(aggregate: ProjectAggregate) {
        aggregate.projectTags[tagId] = ProjectTag(tagId, tagName)
        aggregate.updatedAt = createdAt
    }
}

@DomainEvent(name = TASK_CREATED_EVENT)
class TaskCreatedEvent(
    val projectId: String,
    val taskId: UUID,
    val taskName: String,
    createdAt: Long = System.currentTimeMillis(),
) : Event<ProjectAggregate>(
    name = TASK_CREATED_EVENT,
    aggregateId = projectId,
    createdAt = createdAt
) {
    override fun applyTo(aggregate: ProjectAggregate) {
        aggregate.tasks[taskId] = TaskEntity(taskId, taskName, mutableSetOf())
        aggregate.updatedAt = createdAt
    }
}

@DomainEvent(name = TAG_ASSIGNED_TO_TASK_EVENT)
class TagAssignedToTaskEvent(
    val projectId: String,
    val taskId: UUID,
    val tagId: UUID,
    createdAt: Long = System.currentTimeMillis(),
) : SomeBaseEventWithoutAnnotation(
    name = TAG_ASSIGNED_TO_TASK_EVENT,
    aggregateId = projectId,
    createdAt = createdAt
) {
    override fun applyTo(aggregate: ProjectAggregate) {
        aggregate.tasks[taskId]?.tagsAssigned?.add(tagId)
            ?: throw IllegalArgumentException("No such task: $taskId") // todo sukhoa exception or not?
        aggregate.updatedAt = createdAt
    }
}

// for testing and demo purposes
open class SomeBaseEventWithoutAnnotation (
    name: String,
    aggregateId: String,
    createdAt: Long = System.currentTimeMillis(),
) : Event<ProjectAggregate>(name = name, aggregateId = aggregateId, createdAt = createdAt) {
    override fun applyTo(aggregate: ProjectAggregate) {
        TODO("Not yet implemented")
    }
}
