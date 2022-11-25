package ru.quipy.core

import org.slf4j.LoggerFactory
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.core.exceptions.EventRecordOptimisticLockException
import ru.quipy.database.EventStore
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
    private val eventStore: EventStore
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
     * sent to [EventStore.insertEventRecord] for string it in aggregate event log.
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
        return updateWithSpinLock(aggregateId) { aggregateState, version ->
            tryUpdateState(aggregateState, version, eventGenerationFunction)
        }
    }

    fun updateSerial(aggregateId: ID, eventGenerationFunction: (a: S) -> List<Event<A>>): List<Event<A>> {
        return updateWithSpinLock(aggregateId) { aggregateState, version ->
            tryUpdateState(aggregateState, version, eventGenerationFunction)
        }
    }

    fun getState(aggregateId: ID): S? {
        val versionedState = getVersionedState(aggregateId)
        return if (versionedState.first != 0L) versionedState.second else null
    }

    private fun getVersionedState(aggregateId: ID): Pair<Long, S> {
        var version = 0L
        val state =
            eventStore.findSnapshotByAggregateId(eventSourcingProperties.snapshotTableName, aggregateId)
                ?.let {
                    version = it.version
                    it.snapshot as S
                } ?: aggregateInfo.emptyStateCreator()

        eventStore.findEventRecordsWithAggregateVersionGraterThan(
            aggregateInfo.aggregateEventsTableName,
            aggregateId,
            version
        )
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

    private fun makeSnapshotIfNecessary(aggregateId: ID, aggregateState: S, updatedVersion: Long) {
        if (updatedVersion % eventSourcingProperties.snapshotFrequency == 0L) {
            val snapshot = Snapshot(aggregateId, aggregateState, updatedVersion)
            eventStore.updateSnapshotWithLatestVersion(
                eventSourcingProperties.snapshotTableName,
                snapshot
            ) // todo sukhoa move this and frequency to aggregate-specific config
        }
    }

    private fun <R> updateWithSpinLock(
        aggregateId: ID,
        updateFunction: (aggregateState: S, version: Long) -> R
    ): R {
        var numOfAttempts = 0
        while (true) { // spinlock
            val (currentVersion, aggregateState) = getVersionedState(aggregateId)

            if (currentVersion == 0L) {
                throw IllegalArgumentException("Aggregate ${aggregateInfo.aggregateClass.simpleName} with id: $aggregateId do not exist. Use \"create\" method.")
            }

            try {
                return updateFunction(aggregateState, currentVersion)
            } catch (e: EventRecordOptimisticLockException) {
                logger.info("Optimistic lock exception. Failed to save event records id: ${e.eventRecords.map { it.id }}")

                if (numOfAttempts++ >= 20) // todo sukhoa weird
                    throw IllegalStateException("Too many attempts to save event records: ${e.eventRecords}")

                continue
            }
        }
    }

    private fun <E : Event<A>> tryUpdateState(
        aggregateState: S,
        currentStateVersion: Long,
        eventGenerationFunction: (a: S) -> E
    ): E {
        val eventsGenerationFunction = { a: S ->
            listOf(eventGenerationFunction(a))
        }
        val newEvents: List<E> = tryUpdateState(aggregateState, currentStateVersion, eventsGenerationFunction)
        return newEvents[0]
    }

    private fun <E : Event<A>> tryUpdateState(
        aggregateState: S,
        currentStateVersion: Long,
        eventGenerationFunction: (a: S) -> List<E>
    ): List<E> {
        var updatedVersion = currentStateVersion

        val newEvents = try {
            eventGenerationFunction(aggregateState)
        } catch (e: Exception) {
            logger.warn("Exception thrown during update: ", e)
            throw e
        }

        newEvents.forEach { newEvent ->
            aggregateInfo
                .getStateTransitionFunction(newEvent.name)
                .performTransition(aggregateState, newEvent)
            newEvent.version = ++updatedVersion
        }

        val aggregateId = aggregateState.getId()
            ?: throw IllegalStateException("Aggregate ${aggregateInfo.aggregateClass.simpleName} state has null id after applying events $newEvents")

        val eventRecords = newEvents.map {
            EventRecord(
                "$aggregateId-${it.version}",
                aggregateId,
                it.version,
                it.name,
                eventMapper.eventToString(it)
            )
        }

        try {
            if (eventRecords.size > 1) {
                eventStore.insertEventRecords(aggregateInfo.aggregateEventsTableName, eventRecords)
            } else {
                eventStore.insertEventRecord(aggregateInfo.aggregateEventsTableName, eventRecords[0])
            }
        } catch (e: DuplicateEventIdException) {
            throw EventRecordOptimisticLockException(e.message, e.cause, eventRecords)
        }

        makeSnapshotIfNecessary(aggregateId, aggregateState, updatedVersion)
        return newEvents
    }
}