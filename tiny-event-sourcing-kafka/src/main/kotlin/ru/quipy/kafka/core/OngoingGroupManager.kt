package ru.quipy.kafka.core

import ru.quipy.database.OngoingGroupStorage
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.ExternalEventRecord
import ru.quipy.kafka.registry.DomainGroupRegistry
import ru.quipy.kafka.registry.ExternalEventMapperRegistry
import ru.quipy.mapper.EventMapper
import ru.quipy.mapper.ExternalEventMapper
import java.util.*
import kotlin.reflect.KClass

/**
 * [OngoingGroupManager] is responsible for managing ongoing groups of domain events.
 * It provides methods to convert domain events to external events.
 */
class OngoingGroupManager(
    private val domainGroupRegistry: DomainGroupRegistry,
    private val ongoingGroupStorage: OngoingGroupStorage,
    private val externalEventMapperRegistry: ExternalEventMapperRegistry,
    private val eventMapper: EventMapper,
    private val externalEventMapper: ExternalEventMapper
) {

    private val ongoingGroups = mutableMapOf<String, OngoingGroup<DomainEventsGroup>>()

    suspend fun convertToExternalEventRecords(event: Event<*>, aggregateId: String): List<ExternalEventRecord> {
        val domainGroup = domainGroupRegistry.getGroupFromDomainEvent(event::class)

        return if (domainGroup == null) {
            convertDomainEvent(event)
        } else {
            convertDomainGroup(event, domainGroup, aggregateId)
        }
    }

    private fun convertDomainEvent(event: Event<out Aggregate>): List<ExternalEventRecord> {
        val mapper = externalEventMapperRegistry.getOneToManyMapperFrom(event::class)
        val integrationEventMapper = createEventMapper<DomainEventToExternalEventsMapper<Event<out Aggregate>>>(mapper)

        val externalEvents = integrationEventMapper.toExternalEvents(event)
        return externalEvents.map { createExternalEventRecord(it) }
    }

    private suspend fun convertDomainGroup(
        event: Event<out Aggregate>,
        domainGroup: KClass<DomainEventsGroup>,
        aggregateId: String
    ): List<ExternalEventRecord> {
        ongoingGroups.putIfAbsent(
            aggregateId,
            OngoingGroup(
                domainGroup, domainGroupRegistry, ongoingGroupStorage, eventMapper, aggregateId
            )
        )

        val ongoingGroup = ongoingGroups[aggregateId]!!

        ongoingGroup.addToAggregation(event)

        return ongoingGroup.takeIf { it.isReadyForAggregation() }
            ?.let {
                val mapper = externalEventMapperRegistry.getManyToManyMapperFrom(domainGroup)
                val integrationEventMapper = createGroupMapper<DomainGroupToExternalEventsMapper<DomainEventsGroup>>(mapper)

                val externalEvents = it.mapToExternalEvent { events ->
                    try {
                        integrationEventMapper.toExternalEvents(events as List<DomainEventsGroup>)
                    } catch (e: Exception) {
                        throw e
                    }
                }

                externalEvents.map { createExternalEventRecord(it) }
            }
            ?: emptyList()
    }


    private inline fun <reified T : Any> createEventMapper(mapperClass: KClass<out DomainEventToExternalEventsMapper<out Event<out Aggregate>>>): T {
        return Class.forName(mapperClass.qualifiedName)
            .getDeclaredConstructor()
            .newInstance() as T
    }

    private inline fun <reified T : Any> createGroupMapper(mapperClass: KClass<DomainGroupToExternalEventsMapper<DomainEventsGroup>>): T {
        return Class.forName(mapperClass.qualifiedName)
            .getDeclaredConstructor()
            .newInstance() as T
    }

    private fun createExternalEventRecord(externalEvent: ExternalEvent<*>): ExternalEventRecord {
        return ExternalEventRecord(
            UUID.randomUUID().toString(),
            externalEvent.name,
            externalEventMapper.eventToString(externalEvent)
        )
    }
}
