package ru.quipy.kafka.core

import org.slf4j.LoggerFactory
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

    private val logger = LoggerFactory.getLogger(OngoingGroupManager::class.java)

    private val ongoingGroups = mutableMapOf<String, OngoingGroup<DomainEventsGroup>>()


    fun convertDomainEvent(event: Event<out Aggregate>): List<ExternalEventRecord>? {
        val integrationEventMapper = externalEventMapperRegistry.getOneToManyMapperFrom(event::class)

        val externalEvents = integrationEventMapper?.toExternalEvents(event)
        return externalEvents?.map { createExternalEventRecord(it) }
    }

    suspend fun convertDomainGroup(
        event: Event<out Aggregate>,
        domainGroup: KClass<out DomainEventsGroup>,
        aggregationTable: String
    ): List<ExternalEventRecord> {
        ongoingGroups.putIfAbsent(
            aggregationTable,
            OngoingGroup(
                domainGroup, domainGroupRegistry, ongoingGroupStorage, eventMapper, aggregationTable
            )
        )

        val ongoingGroup = ongoingGroups[aggregationTable]!!

        ongoingGroup.addToAggregation(event)

        return ongoingGroup.takeIf { it.isReadyForAggregation() }
            ?.let {
                val integrationEventMapper =
                    externalEventMapperRegistry.getManyToManyMapperFrom(domainGroup)

                val externalEvents = it.mapToExternalEvent { events ->
                    try {
                        integrationEventMapper?.toExternalEvents(events as List<DomainEventsGroup>) ?: emptyList()
                    } catch (e: Exception) {
                        logger.error("Error while processing domain group: ${e.message}", e)
                        throw e
                    }
                }

                externalEvents.map { createExternalEventRecord(it) }
            }
            ?: emptyList()
    }

    fun removeAggregationTable(aggregationTable: String) {
        ongoingGroups[aggregationTable]?.removeTable()
    }

    private fun createExternalEventRecord(externalEvent: ExternalEvent<*>): ExternalEventRecord {
        return ExternalEventRecord(
            UUID.randomUUID().toString(),
            externalEvent.name,
            externalEventMapper.eventToString(externalEvent)
        )
    }
}
