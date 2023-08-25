package ru.quipy.kafka.core

import ru.quipy.database.OngoingGroupStorage
import ru.quipy.domain.*
import ru.quipy.mapper.EventMapper
import ru.quipy.kafka.registry.DomainGroupRegistry
import kotlin.reflect.KClass

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
