package ru.quipy.kafka.core

import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic

interface DomainEventsGroup

interface DomainEventToExternalEventsMapper<E :  Event<out Aggregate>> {

    fun toExternalEvents(domainEvent: E): List<ExternalEvent<out Topic>>
}

interface DomainGroupToExternalEventsMapper<G : DomainEventsGroup> {

    fun toExternalEvents(domainEvents: List<G>): List<ExternalEvent<out Topic>>
}