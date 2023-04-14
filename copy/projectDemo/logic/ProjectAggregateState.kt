package ru.quipy.projectDemo.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.projectDemo.api.*
import java.util.*

// Service's business logic
class ProjectAggregateState: AggregateState<String, ProjectAggregate> {
    private lateinit var projectId: String
    var createdAt: Long = System.currentTimeMillis()
    var updatedAt: Long = System.currentTimeMillis()

    var tasks = mutableMapOf<UUID, TaskEntity>()
    var projectTags = mutableMapOf<UUID, ProjectTag>()

    override fun getId() = projectId

    // State transition functions
    @StateTransitionFunc
    fun projectCreatedApply(event: ProjectCreatedEvent) {
        projectId = event.projectId
        updatedAt = createdAt
    }

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
