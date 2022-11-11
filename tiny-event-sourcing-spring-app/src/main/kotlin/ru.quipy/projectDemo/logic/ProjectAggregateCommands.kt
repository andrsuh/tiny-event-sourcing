package ru.quipy.projectDemo

import ru.quipy.projectDemo.api.ProjectCreatedEvent
import ru.quipy.projectDemo.api.TagAssignedToTaskEvent
import ru.quipy.projectDemo.api.TagCreatedEvent
import ru.quipy.projectDemo.api.TaskCreatedEvent
import ru.quipy.projectDemo.logic.ProjectAggregateState
import java.util.*


// Commands : takes something -> returns event
fun ProjectAggregateState.create(id: String): ProjectCreatedEvent {
    return ProjectCreatedEvent(projectId = id)
}

fun ProjectAggregateState.addTask(name: String): TaskCreatedEvent {
    return TaskCreatedEvent(projectId = this.getId(), taskId = UUID.randomUUID(), taskName = name)
}

fun ProjectAggregateState.createTag(name: String): TagCreatedEvent {
    if (projectTags.values.any { it.name == name }) {
        throw IllegalArgumentException("Tag already exists: $name")
    }
    return TagCreatedEvent(projectId = this.getId(), tagId = UUID.randomUUID(), tagName = name)
}

fun ProjectAggregateState.assignTagToTask(tagId: UUID, taskId: UUID): TagAssignedToTaskEvent {
    if (!projectTags.containsKey(tagId)) {
        throw IllegalArgumentException("Tag doesn't exists: $tagId")
    }

    if (!tasks.containsKey(taskId)) {
        throw IllegalArgumentException("Task doesn't exists: $taskId")
    }

    return TagAssignedToTaskEvent(projectId = this.getId(), tagId = tagId, taskId = taskId)
}