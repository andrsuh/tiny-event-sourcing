package ru.quipy.core

import org.slf4j.LoggerFactory
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.core.exceptions.EventRecordOptimisticLockException
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*
import ru.quipy.mapper.EventMapper
import kotlin.reflect.KClass

/**
 * Allows you to update aggregates and get the last state of the aggregate instances.
 */
class EventSourcingService<ID : Any, A : Aggregate, S : AggregateState<ID, A>>(
    aggregateClass: KClass<A>,
    aggregateRegistry: AggregateRegistry,
    private val eventMapper: EventMapper,
    private val eventSourcingProperties: EventSourcingProperties,
    private val eventStoreDbOperations: EventStoreDbOperations
) {
    companion object {
        private val logger = LoggerFactory.getLogger(EventSourcingService::class.java)
    }

    private val aggregateInfo =
        aggregateRegistry.getStateTransitionInfo<ID, A, S>(aggregateClass)
            ?: throw IllegalArgumentException("Aggregate $aggregateClass is not registered")

    fun <E : Event<A>> create(eventGenerationFunction: (a: S) -> E): E {
        val emptyAggregateState = aggregateInfo.emptyStateCreator.invoke()
        return tryUpdateState(emptyAggregateState, 0, eventGenerationFunction)
    }

    /**
     * Allows to update aggregate by running some logic on aggregate state that results in [Event].
     *
     * [aggregateId] - refers to the ID of the aggregate instance for update. If no aggregate instance with
     * given id found then IllegalArgumentException will be thrown.
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
    fun <E : Event<A>> update(aggregateId: ID, eventGenerationFunction: (a: S) -> E): E {
        var numOfAttempts = 0
        while (true) { // spinlock
            val (currentVersion, aggregateState) = getVersionedState(aggregateId)

            if (currentVersion == 0L) {
                throw IllegalArgumentException("Aggregate ${aggregateInfo.aggregateClass.simpleName} with id: $aggregateId do not exist. Use \"create\" method.")
            }

            try {
                return tryUpdateState(aggregateState, currentVersion, eventGenerationFunction)
            } catch (e: EventRecordOptimisticLockException) {
                logger.info("Optimistic lock exception. Failed to save event record id: ${e.eventRecord.id}")

                if (numOfAttempts++ >= 20) // todo sukhoa weird
                    throw IllegalStateException("Too many attempts to save event record: ${e.eventRecord}, event: ${e.eventRecord.payload}")

                continue
            }
        }
    }

    private fun <E : Event<A>> tryUpdateState(
        aggregateState: S,
        currentStateVersion: Long,
        eventGenerationFunction: (a: S) -> E
    ): E {
        val updatedVersion = currentStateVersion + 1

        val newEvent = try {
            eventGenerationFunction(aggregateState)
        } catch (e: Exception) {
            logger.warn("Exception thrown during update: ", e)
            throw e
        }

        aggregateInfo
            .getStateTransitionFunction(newEvent.name)
            .performTransition(aggregateState, newEvent)

        val aggregateId = aggregateState.getId()
            ?: throw IllegalStateException("Aggregate ${aggregateInfo.aggregateClass.simpleName} state has null id after applying event $newEvent")

        newEvent.version = updatedVersion

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
            throw EventRecordOptimisticLockException(e.message, e.cause, eventRecord)
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

    fun getState(aggregateId: ID): S? {
        val versionedState = getVersionedState(aggregateId)
        return if (versionedState.first != 0L) versionedState.second else null
    }

    fun getStateOfVersion(aggregateId: ID, version: Long): S? {
        val versionedState = getVersionedState(aggregateId, version)
        return if (versionedState.first != 0L) versionedState.second else null
    }

    private fun getVersionedState(aggregateId: ID, certainVersion: Long? = null): Pair<Long, S> {
        var version = 0L

        val (initialState, initialVersion) = getInitialState(aggregateId)

        val state = when {
            certainVersion != null -> aggregateInfo.emptyStateCreator()
            else -> {
                version = initialVersion
                initialState
            }
        }

        eventStoreDbOperations.findEventRecordsWithAggregateVersionGraterThan(
            aggregateInfo.aggregateEventsTableName,
            aggregateId,
            version
        )
            .filter { eventRecord -> certainVersion == null || eventRecord.aggregateVersion <= certainVersion }
            .map { eventRecord ->
                eventMapper.toEvent(
                    eventRecord.payload,
                    aggregateInfo.getEventTypeByName(eventRecord.eventTitle)
                )
            }.forEach { event ->
                aggregateInfo
                    .getStateTransitionFunction(event.name)
                    .performTransition(state, event)

                version++
            }

        return version to state
    }

    private fun getInitialState(aggregateId: ID): Pair<S, Long> =
        eventStoreDbOperations.findSnapshotByAggregateId(eventSourcingProperties.snapshotTableName, aggregateId)
            ?.let {
                it.snapshot as S to it.version
            } ?: (aggregateInfo.emptyStateCreator() to 0L)

}