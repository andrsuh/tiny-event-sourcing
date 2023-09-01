package ru.quipy.api.external

import ru.quipy.api.internal.ProjectCreatedEvent
import ru.quipy.api.internal.TagCreatedEvent
import ru.quipy.api.internal.TaskCreatedEvent
import ru.quipy.api.internal.taskAndTagEventsGroup
import ru.quipy.core.annotations.ExternalEventsMapper
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.DomainEventToExternalEventsMapper
import ru.quipy.kafka.core.DomainGroupToExternalEventsMapper

@ExternalEventsMapper
class ProjectCreatedToExternalEventMapper : DomainEventToExternalEventsMapper<ProjectCreatedEvent> {

    override fun toExternalEvents(domainEvent: ProjectCreatedEvent): List<ExternalEvent<out Topic>> {
        return listOf(
            ProjectCreatedExternalEvent(
                title = domainEvent.title + " by " + domainEvent.creatorId,
                creatorId = domainEvent.creatorId
            )
        )
    }
}

@ExternalEventsMapper
class TaskAndTagCreatedToExternalEventMapper : DomainGroupToExternalEventsMapper<taskAndTagEventsGroup> {

    override fun toExternalEvents(domainEvents: List<taskAndTagEventsGroup>): List<ExternalEvent<out Topic>> {
        var tagName: String? = null
        var taskName: String? = null


        for (event in domainEvents) {
            when (event) {
                is TaskCreatedEvent -> {
                    taskName = event.taskName
                }

                is TagCreatedEvent -> {
                    tagName = event.tagName
                }
            }
        }

        return listOf(
            TaskAndTagCreatedExternalEvent(
                taskName = taskName!!,
                tagName = tagName!!
            )
        )
    }
}
