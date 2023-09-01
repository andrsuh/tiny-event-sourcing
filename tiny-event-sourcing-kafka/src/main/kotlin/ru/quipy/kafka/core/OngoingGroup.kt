package ru.quipy.kafka.core

import ru.quipy.database.OngoingGroupStorage
import ru.quipy.domain.*
import ru.quipy.kafka.registry.DomainGroupRegistry
import ru.quipy.mapper.EventMapper
import kotlin.reflect.KClass

/**
 * [OngoingGroup] represents a group of domain events which are involved in the mapping to external events.
 * It provides methods to add events to the group, check eligibility for aggregation, and map the group to external events.
 *
 * [OngoingGroup] performs the following tasks:
 * - Adds events to the aggregation table if they are eligible for aggregation.
 * - Checks if an event is eligible for aggregation based on the domain events group.
 * - Checks if the ongoing group is ready for aggregation.
 * - Maps the events in the ongoing group to external events using a provided event processing function.
 */
class OngoingGroup<G : DomainEventsGroup>(
    private val domainGroup: KClass<G>,
    private val domainGroupRegistry: DomainGroupRegistry,
    private val ongoingGroupStorage: OngoingGroupStorage,
    private val eventMapper: EventMapper,
    aggregateId: String
) {

    private val aggregationTable: String = ("aggregation-${domainGroup.simpleName}-$aggregateId").lowercase()

    fun addToAggregation(event: Event<*>) {
        if (isEligibleForAggregation(event)) {
            ongoingGroupStorage.insertEventAggregation(
                aggregationTable,
                EventAggregation(event::class.qualifiedName.toString(), eventMapper.eventToString(event))
            )
        }
    }

    private fun isEligibleForAggregation(event: Event<*>): Boolean {
        val domainGroup = domainGroupRegistry.getDomainEventsFromDomainGroup(domainGroup)
        if (!domainGroup.contains(event::class)) {
            return false;
        }

        val pollEvents = ongoingGroupStorage.findBatchOfEventAggregations(aggregationTable)
        return pollEvents.size != domainGroup.size
    }

    fun isReadyForAggregation(): Boolean {
        val expectedEvents = domainGroupRegistry.getDomainEventsFromDomainGroup(domainGroup)
        val pollEvents = ongoingGroupStorage.findBatchOfEventAggregations(aggregationTable)

        return pollEvents.size == expectedEvents.size
    }

    suspend fun mapToExternalEvent(eventProcessingFunction: suspend (List<Event<*>>) -> List<ExternalEvent<out Topic>>): List<ExternalEvent<out Topic>> {
        val pollEvents = ongoingGroupStorage.findBatchOfEventAggregations(aggregationTable)
        val events = pollEvents.map {
            eventMapper.toEvent(it.payload, Class.forName(it.eventTitle).kotlin as KClass<out Event<Aggregate>>)
        }

        ongoingGroupStorage.dropTable(aggregationTable)

        return eventProcessingFunction(events)
    }
}
