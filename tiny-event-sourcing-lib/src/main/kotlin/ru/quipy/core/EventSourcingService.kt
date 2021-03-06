package ru.quipy.core

import org.slf4j.LoggerFactory
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.domain.EventRecord
import ru.quipy.domain.Snapshot
import ru.quipy.mapper.EventMapper
import kotlin.reflect.KClass

/**
 * Allows you to update aggregates and get the last state of the aggregate instances.
 */
class EventSourcingService<A : Aggregate>(
    aggregateClass: KClass<A>,
    aggregateRegistry: AggregateRegistry,
    private val eventMapper: EventMapper,
    private val eventSourcingProperties: EventSourcingProperties,
    private val eventStoreDbOperations: EventStoreDbOperations
) {
    companion object {
        private val logger = LoggerFactory.getLogger(EventSourcingService::class.java)
    }

    private val aggregateInfo = aggregateRegistry.getAggregateInfo(aggregateClass) // todo sukhoa pass the only info?
        ?: throw IllegalArgumentException("Aggregate $aggregateClass is not registered")

    /**
     * Allows to update aggregate by running some logic on aggregate state that results in [Event].
     *
     * [aggregateId] - refers to the ID of the aggregate instance for update. If no aggregate instance with
     * given id found then will be created.
     * [eventGenerationFunction] - method will construct the current aggregate instance state and pass it to this function.
     * You can run any logic/validations/invariants checks on this aggregate state and return the [Event] instance if case
     * everything is fine and update can be done. Just throw exception with a descriptive message otherwise e.g. "Name can't be empty".
     *
     * Your event will be applied to the aggregate to check if it is applicable indeed. Then event will be serialized to
     * string using [EventMapper], wrapped with [EventRecord] instance containing all the necessary meta-information and
     * sent to [EventStoreDbOperations.insertEventRecord] for string it in aggregate event log.
     *
     * If insertion failed with [DuplicateEventIdException], which means some other process managed to store event with same
     * version/aggregateId first, we will repeat all the actions starting with loading and construction the freshest
     * aggregate instance state and so on. This is how the concurrency issues are handled. This guarantees that replaying the
     * events from log in the insertion order we will get exactly the same aggregate state as what we got before insertion.
     *
     * The number of attempts is limited to 20 currently.
     *
     */
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
            newEvent.aggregateId = aggregateId

            val eventRecord = EventRecord(
                "$aggregateId-$updatedVersion",
                aggregateId,
                updatedVersion,
                newEvent.name,
                eventMapper.eventToString(newEvent)
            )

            try {
                eventStoreDbOperations.insertEventRecord(aggregateInfo.aggregateEventsTableName, eventRecord)
            } catch (e: DuplicateEventIdException) {
                logger.info("Optimistic lock exception. Failed to save event record id: ${eventRecord.id}")

                if (numOfAttempts++ >= 20)
                    throw IllegalStateException("Too many attempts to save event record: $eventRecord, event: ${eventRecord.payload}")

                continue
            }

            if (updatedVersion % eventSourcingProperties.snapshotFrequency == 0L) {
                val snapshot = Snapshot(aggregateId, aggregateState, updatedVersion)
                eventStoreDbOperations.updateSnapshotWithLatestVersion(
                    eventSourcingProperties.snapshotTableName,
                    snapshot
                ) // todo sukhoa move this and frequency to aggregate-specific config
            }

            return newEvent
        }
    }

    fun getState(aggregateId: String): A = getVersionedState(aggregateId).second

    private fun getVersionedState(aggregateId: String): Pair<Long, A> {
        var version = 0L

        val aggregate =
            eventStoreDbOperations.findSnapshotByAggregateId(eventSourcingProperties.snapshotTableName, aggregateId)
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