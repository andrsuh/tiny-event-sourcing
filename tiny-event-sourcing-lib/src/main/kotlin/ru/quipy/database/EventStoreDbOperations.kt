package ru.quipy.database

import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot

interface EventStoreDbOperations {

    @Throws(exceptionClasses = [DuplicateEventIdException::class])
    fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord)

    fun findEventRecordsByAggregateId(aggregateTableName: String, aggregateId: String): List<EventRecord>

    fun findOneEventRecordById(aggregateTableName: String, eventRecordId: Long): EventRecord?

    fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: String,
        aggregateVersion: Long
    ): List<EventRecord>

    fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        eventRecordId: Long,
        batchSize: Int
    ): List<EventRecord>

    fun findOneEventRecordAfter(aggregateTableName: String, readTimestamp: Long): EventRecord?

    fun findBatchOfEventRecordAfter(aggregateTableName: String, readTimestamp: Long, batchSize: Int): List<EventRecord>

    fun tableExists(aggregateTableName: String): Boolean

    fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot)

    fun findSnapshotByAggregateId(snapshotsTableName: String, aggregateId: String): Snapshot?

    fun findStreamReadIndex(streamName: String): EventStreamReadIndex?

    fun getActiveStreamReader(streamName: String): ActiveEventStreamReader?

    fun commitStreamReadIndex(readIndex: EventStreamReadIndex)
}