package ru.quipy.core

import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*
import ru.quipy.mapper.EventMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DuplicateKeyException
import kotlin.reflect.KClass


class EventSourcingService<A : Aggregate>(
    aggregateClass: KClass<A>,
    aggregateRegistry: AggregateRegistry,
    private val eventMapper: EventMapper,
    private val configProperties: ConfigProperties,
    private val eventStoreDbOperations: EventStoreDbOperations
) {
    companion object {
        private val logger = LoggerFactory.getLogger(EventSourcingService::class.java)
    }

    private val aggregateInfo = aggregateRegistry.getAggregateInfo(aggregateClass) // todo sukhoa pass the only info
        ?: throw IllegalArgumentException("Aggregate $aggregateClass is not registered")

    fun <E : Event<A>> update(aggregateId: String, eventGenerationFunction: (a: A) -> E): E {
        var numOfAttempts = 0
        while (true) { // spinlock
            val (currentVersion, aggregateState) = getVersionedState(aggregateId)

            val updatedVersion = currentVersion + 1

            val newEvent = try {
                eventGenerationFunction(aggregateState)
            } catch (e: Exception) {
                logger.warn("Exception thrown during update: ", e)
                throw e
            }

            newEvent applyTo aggregateState
            newEvent.version = updatedVersion
            //newEvent.aggregateId = aggregateId todo sukhoa think of it

            val eventRecord = EventRecord(
                "$aggregateId-$updatedVersion",
                aggregateId,
                updatedVersion,
                newEvent.name,
                eventMapper.eventToString(newEvent)
            )

            try {
                eventStoreDbOperations.insertEventRecord(aggregateInfo.aggregateEventsTableName, eventRecord)
            } catch (e: DuplicateKeyException) {
                logger.info("Optimistic lock exception. Failed to save event wrapper id: ${eventRecord.id}")

                if (numOfAttempts++ >= 20)
                    throw IllegalStateException("Too many attempts to save event record: $eventRecord, event: ${eventRecord.payload}")

                continue
            }

            if (updatedVersion % configProperties.snapshotFrequency == 0L) {
                val snapshot = Snapshot(aggregateId, aggregateState, updatedVersion)
                eventStoreDbOperations.updateSnapshotWithLatestVersion(configProperties.snapshotTableName, snapshot) // todo sukhoa move this and frequency to aggregate-specific config
            }

            return newEvent
        }
    }

    fun getState(aggregateId: String): A = getVersionedState(aggregateId).second

    fun getVersionedState(aggregateId: String): Pair<Long, A> {
        var version = 0L

        val aggregate =
            eventStoreDbOperations.findSnapshotByAggregateId(configProperties.snapshotTableName, aggregateId)
                ?.let {
                    version = it.version
                    it.snapshot as A
                }
                ?: aggregateInfo.instantiateFunction(aggregateId)


        eventStoreDbOperations.findEventRecordsWithAggregateVersionGraterThan(
            aggregateInfo.aggregateEventsTableName,
            aggregateId,
            version
        )
            .map { eventRecord ->
                eventMapper.toEvent<A>(
                    eventRecord.payload,
                    aggregateInfo.getEventTypeByName(eventRecord.eventTitle)
                )
            }.forEach { event ->
                event applyTo aggregate
                version++
            }

        return version to aggregate
    }
}