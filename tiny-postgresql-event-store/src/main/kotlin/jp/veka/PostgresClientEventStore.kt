package jp.veka

import jp.veka.converter.ResultSetToEntityMapper
import jp.veka.db.factory.ConnectionFactory
import jp.veka.exception.UnknownEntityClassException
import jp.veka.executor.QueryExecutor
import jp.veka.query.Query
import jp.veka.query.QueryBuilder
import jp.veka.query.select.SelectQuery
import jp.veka.tables.ActiveEventStreamReaderDto
import jp.veka.tables.EventRecordDto
import jp.veka.tables.EventRecordTable
import jp.veka.tables.EventStreamActiveReadersTable
import jp.veka.tables.EventStreamReadIndexDto
import jp.veka.tables.EventStreamReadIndexTable
import jp.veka.tables.SnapshotDto
import jp.veka.tables.SnapshotTable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import java.sql.ResultSet
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
            QueryBuilder.insert(eventStoreSchemaName, EventRecordTable.name, EventRecordDto(eventRecord, aggregateTableName))
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
            QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, SnapshotTable.name, SnapshotDto(snapshot, tableName))
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
            QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, EventStreamActiveReadersTable.name, ActiveEventStreamReaderDto(updatedActiveReader))
        )
    }

    override fun tryReplaceActiveStreamReader(
        expectedVersion: Long,
        newActiveReader: ActiveEventStreamReader
    ): Boolean {
       return executor.executeReturningBoolean(
           QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, EventStreamActiveReadersTable.name, ActiveEventStreamReaderDto(newActiveReader))
               .andWhere("${EventStreamActiveReadersTable.name}.${EventStreamActiveReadersTable.version.name} = $expectedVersion")
       )
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex): Boolean {
        return executor.executeReturningBoolean(
            QueryBuilder.insertOrUpdateQuery(eventStoreSchemaName, EventStreamReadIndexTable.name, EventStreamReadIndexDto(readIndex))
        )
    }

    private fun <T: Any> findEntityById(id: Any, clazz: KClass<T>) : T? {
        val (tableName, tableIdColumnName) = when(clazz) {
            EventRecord::class -> EventRecordTable.name to EventRecordTable.id.name
            Snapshot::class -> SnapshotTable.name to SnapshotTable.id.name
            EventStreamReadIndex::class -> EventStreamReadIndexTable.name to EventStreamReadIndexTable.id.name
            ActiveEventStreamReader::class -> EventStreamActiveReadersTable.name to EventStreamActiveReadersTable.id.name
            else -> throw UnknownEntityClassException(clazz.simpleName)
        }
        val query = QueryBuilder.select(eventStoreSchemaName, tableName)
            .andWhere("$tableIdColumnName = '$id'")
            .limit(1)

        val result = executor.executeReturningResultSet(query)
        return resultSetToEntityMapper.convert(result, clazz)
    }
}