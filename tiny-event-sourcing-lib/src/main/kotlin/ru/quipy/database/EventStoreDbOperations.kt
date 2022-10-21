package ru.quipy.database

import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot

/**
 * Abstracts away the DB access. Provides the operations for event sourcing functioning.
 * You can provide your own implementation of [EventStoreDbOperations] and run event sourcing app
 * working on any DB you wish under the hood.
 */
interface EventStoreDbOperations {

    /**
     * Appends event record in aggregate event log.
     *
     * Throws [DuplicateEventIdException] if there is already event with same.
     *
     * This is used to handle concurrency If one of the insertion operation running in parallel managed to insert
     * event, then others should retry their attempt including create aggregate state again, performing validations
     * and insertion the resulting event. Some kind of optimistic concurrency control.
     *
     * We suggest using same (aggregateId, version) combination as a sign of the event is duplicate (version is a
     * monotonically increasing sequence always incremented by one). In this case there will be no two events with same
     * version within single aggregate instance. That allows you to tackle concurrency issues withing same aggregate instance.
     * So that execution will be linearized for every single aggregate instance.
     *
     */
    @Throws(exceptionClasses = [DuplicateEventIdException::class])
    fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord)

    /**
     * Aggregate state version is the number of events that should be applied to empty aggregate state to get current state.
     *
     * Each event stores the number of the aggregate state version. This is the version of the state after the event is applied.
     */
    fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: Any,
        aggregateVersion: Long
    ): List<EventRecord>

    /**
     * Returns a batch of events that has their sequence number greater than passed
     */
    fun findBatchOfEventRecordAfter(aggregateTableName: String, eventSequenceNum: Long, batchSize: Int): List<EventRecord>

    fun tableExists(aggregateTableName: String): Boolean

    fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot)

    fun findSnapshotByAggregateId(snapshotsTableName: String, aggregateId: Any): Snapshot?

    fun findStreamReadIndex(streamName: String): EventStreamReadIndex?

    fun getActiveStreamReader(streamName: String): ActiveEventStreamReader?

    fun commitStreamReadIndex(readIndex: EventStreamReadIndex)
}