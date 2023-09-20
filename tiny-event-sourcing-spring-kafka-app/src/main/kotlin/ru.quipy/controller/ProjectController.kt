package ru.quipy.controller

import org.springframework.web.bind.annotation.*
import ru.quipy.api.internal.ProjectAggregate
import ru.quipy.api.internal.ProjectCreatedEvent
import ru.quipy.api.internal.TagCreatedEvent
import ru.quipy.api.internal.TaskCreatedEvent
import ru.quipy.core.EventSourcingService
import ru.quipy.logic.ProjectAggregateState
import ru.quipy.logic.addTask
import ru.quipy.logic.create
import ru.quipy.logic.createTag
import java.util.*

@RestController
@RequestMapping("/projects")
class ProjectController(
    val projectEsService: EventSourcingService<UUID, ProjectAggregate, ProjectAggregateState>
) {

    @PostMapping("/{projectTitle}")
    fun createProject(@PathVariable projectTitle: String, @RequestParam creatorId: String): ProjectCreatedEvent {
        return projectEsService.create { it.create(UUID.randomUUID(), projectTitle, creatorId) }
    }

    @GetMapping("/{projectId}")
    fun getProject(@PathVariable projectId: UUID): ProjectAggregateState? {
        return projectEsService.getState(projectId)
    }

    @PostMapping("/{projectId}/tasks/{taskName}")
    fun createTask(@PathVariable projectId: UUID, @PathVariable taskName: String): TaskCreatedEvent {
        return projectEsService.update(projectId) {
            it.addTask(taskName)
        }
    }

    @PostMapping("/{projectId}/tags/{tagName}")
    fun createTag(@PathVariable projectId: UUID, @PathVariable tagName: String): TagCreatedEvent {
        return projectEsService.update(projectId) {
            it.createTag(tagName)
        }
    }
}