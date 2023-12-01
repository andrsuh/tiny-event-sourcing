package jp.veka

import jp.veka.converter.ResultSetToEntityMapper
import jp.veka.executor.QueryExecutor
import jp.veka.query.QueryBuilder
import jp.veka.tables.ActiveEventStreamReaderDto
import jp.veka.tables.EventRecordDto
import jp.veka.tables.EventRecordTable
import jp.veka.tables.EventStreamActiveReadersTable
import jp.veka.tables.EventStreamReadIndexDto
import jp.veka.tables.SnapshotDto
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import kotlin.reflect.KClass

class PostgresClientEventStore(
    private val eventStoreSchemaName: String,
    private val resultSetToEntityMapper: ResultSetToEntityMapper,
    private val executor: QueryExecutor
) : EventStore {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(PostgresClientEventStore::class.java)
    }
    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        executor.execute(
            QueryBuilder.insert(eventStoreSchemaName, EventRecordDto(eventRecord, aggregateTableName))
        )
    }

    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        executor.execute(
            QueryBuilder.batchInsert(eventStoreSchemaName, EventRecordTable.name, eventRecords.map { EventRecordDto(it, aggregateTableName) })
        )
    }

    override fun tableExists(aggregateTableName: String): Boolean {
        return true // ???
    }

    override fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot) {
        executor.execute(
            QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, SnapshotDto(snapshot, tableName))
        )
    }

    override fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: Any,
        aggregateVersion: Long
    ): List<EventRecord> {
        val query = QueryBuilder.select(eventStoreSchemaName, EventRecordTable.name)
            .andWhere("${EventRecordTable.aggregateId.name} = '$aggregateId'")
            .andWhere("${EventRecordTable.aggregateTableName.name} = '$aggregateTableName'")
            .andWhere("${EventRecordTable.aggregateVersion.name} > $aggregateVersion")
        val result = executor.executeReturningResultSet(query)
        return resultSetToEntityMapper.convertMany(result, EventRecord::class)
    }

    override fun findBatchOfEventRecordAfter(
        aggregateTableName: String,
        eventSequenceNum: Long,
        batchSize: Int
    ): List<EventRecord> {
        val query = QueryBuilder.select(eventStoreSchemaName, EventRecordTable.name)
            .andWhere("${EventRecordTable.aggregateTableName.name} = '$aggregateTableName'")
            .andWhere("${EventRecordTable.createdAt.name} > $eventSequenceNum") // id > $eventSequenceNum ??
            .limit(batchSize)
        val result = executor.executeReturningResultSet(query)
        return resultSetToEntityMapper.convertMany(result, EventRecord::class)
    }
    override fun findSnapshotByAggregateId(snapshotsTableName: String, aggregateId: Any): Snapshot? {
        return findEntityById(aggregateId, Snapshot::class)
    }

    override fun findStreamReadIndex(streamName: String): EventStreamReadIndex? {
        return findEntityById(streamName, EventStreamReadIndex::class)
    }

    override fun getActiveStreamReader(streamName: String): ActiveEventStreamReader? {
        return findEntityById(streamName, ActiveEventStreamReader::class)
    }

    override fun tryUpdateActiveStreamReader(updatedActiveReader: ActiveEventStreamReader): Boolean {
        return executor.executeReturningBoolean(
            QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, ActiveEventStreamReaderDto(updatedActiveReader))
        )
    }

    override fun tryReplaceActiveStreamReader(
        expectedVersion: Long,
        newActiveReader: ActiveEventStreamReader
    ): Boolean {
       return executor.executeReturningBoolean(
           QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, ActiveEventStreamReaderDto(newActiveReader))
               .andWhere("${EventStreamActiveReadersTable.name}.${EventStreamActiveReadersTable.version.name} = $expectedVersion")
       )
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex): Boolean {
        return executor.executeReturningBoolean(
            QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, EventStreamReadIndexDto(readIndex))
        )
    }

    private fun <T: Any> findEntityById(id: Any, clazz: KClass<T>) : T? {
        var query = QueryBuilder.findEntityByIdQuery(eventStoreSchemaName, id, clazz)
        val result = executor.executeReturningResultSet(query)
        return resultSetToEntityMapper.convert(result, clazz)
    }
}