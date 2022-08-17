package ru.quipy.demo.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.demo.api.ProjectAggregate
import ru.quipy.demo.api.TagAssignedToTaskEvent
import ru.quipy.demo.api.TagCreatedEvent
import ru.quipy.demo.api.TaskCreatedEvent
import ru.quipy.domain.AggregateState
import java.util.*

// Service's business logic
data class ProjectAggregateState(
    override val aggregateId: String
) : AggregateState<String, ProjectAggregate> {
    override var createdAt: Long = System.currentTimeMillis()
    override var updatedAt: Long = System.currentTimeMillis()

    var tasks = mutableMapOf<UUID, TaskEntity>()
    var projectTags = mutableMapOf<UUID, ProjectTag>()

    // State transition functions
    @StateTransitionFunc
    fun tagCreatedApply(event: TagCreatedEvent) {
        projectTags[event.tagId] = ProjectTag(event.tagId, event.tagName)
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun taskCreatedApply(event: TaskCreatedEvent) {
        tasks[event.taskId] = TaskEntity(event.taskId, event.taskName, mutableSetOf())
        updatedAt = createdAt
    }
}

data class TaskEntity(
    val id: UUID = UUID.randomUUID(),
    val name: String,
    val tagsAssigned: MutableSet<UUID>
)

data class ProjectTag(
    val id: UUID = UUID.randomUUID(),
    val name: String
)

@StateTransitionFunc
fun ProjectAggregateState.tagAssignedApply(event: TagAssignedToTaskEvent) {
    tasks[event.taskId]?.tagsAssigned?.add(event.tagId)
        ?: throw IllegalArgumentException("No such task: ${event.taskId}") // todo sukhoa exception or not?
    updatedAt = createdAt
}
