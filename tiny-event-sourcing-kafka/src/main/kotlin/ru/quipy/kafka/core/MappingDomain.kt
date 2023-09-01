package ru.quipy.kafka.core

import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic

/**
 * [DomainEventsGroup] represents a group of domain events.
 * It serves as a marker interface to indicate that a class represents a collection or group of domain events.
 */
interface DomainEventsGroup

/**
 * [DomainEventToExternalEventsMapper] defines a mapper that converts a domain event of an [Aggregate] into a list of external events.
 * The external events are associated with a [Topic].
 *
 * @param E is a type of the domain event.
 */
interface DomainEventToExternalEventsMapper<E :  Event<out Aggregate>> {

    fun toExternalEvents(domainEvent: E): List<ExternalEvent<out Topic>>
}

/**
 * [DomainGroupToExternalEventsMapper] defines a mapper that converts a list of domain events into a list of external events.
 * The external events are associated with a [Topic].
 *
 * @param G The type of the domain events group. It should implement the DomainEventsGroup interface.
 */
interface DomainGroupToExternalEventsMapper<G : DomainEventsGroup> {

    fun toExternalEvents(domainEvents: List<G>): List<ExternalEvent<out Topic>>
}
