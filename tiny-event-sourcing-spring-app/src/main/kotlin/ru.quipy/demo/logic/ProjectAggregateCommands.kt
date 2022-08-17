package ru.quipy.demo

import ru.quipy.demo.api.TagAssignedToTaskEvent
import ru.quipy.demo.api.TagCreatedEvent
import ru.quipy.demo.api.TaskCreatedEvent
import ru.quipy.demo.logic.ProjectAggregateState
import java.util.*


// Commands : takes something -> returns event
fun ProjectAggregateState.addTask(name: String): TaskCreatedEvent {
    return TaskCreatedEvent(projectId = this.aggregateId, taskId = UUID.randomUUID(), taskName = name)
}

fun ProjectAggregateState.createTag(name: String): TagCreatedEvent {
    if (projectTags.values.any { it.name == name }) {
        throw IllegalArgumentException("Tag already exists: $name")
    }
    return TagCreatedEvent(projectId = this.aggregateId, tagId = UUID.randomUUID(), tagName = name)
}

fun ProjectAggregateState.assignTagToTask(tagId: UUID, taskId: UUID): TagAssignedToTaskEvent {
    if (!projectTags.containsKey(tagId)) {
        throw IllegalArgumentException("Tag doesn't exists: $tagId")
    }

    if (!tasks.containsKey(taskId)) {
        throw IllegalArgumentException("Task doesn't exists: $taskId")
    }

    return TagAssignedToTaskEvent(projectId = this.aggregateId, tagId = tagId, taskId = taskId)
}