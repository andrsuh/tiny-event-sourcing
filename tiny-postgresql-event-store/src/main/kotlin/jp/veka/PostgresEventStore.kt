package jp.veka

import jp.veka.converter.ResultSetToEntityMapper
import jp.veka.exception.UnknownEntityException
import jp.veka.executor.ExceptionLoggingSqlQueriesExecutor
import jp.veka.factory.PostgresConnectionFactory
import jp.veka.query.QueryBuilder
import jp.veka.query.Query
import jp.veka.query.select.SelectQuery
import jp.veka.tables.ActiveEventStreamReaderDto
import jp.veka.tables.EventRecordDto
import jp.veka.tables.EventRecordTable
import jp.veka.tables.EventStreamActiveReadersTable
import jp.veka.tables.EventStreamReadIndexDto
import jp.veka.tables.EventStreamReadIndexTable
import jp.veka.tables.SnapshotDto
import jp.veka.tables.SnapshotTable
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import java.sql.ResultSet
import kotlin.reflect.KClass

class PostgresEventStore(
    private val databaseConnectionFactory: PostgresConnectionFactory,
    private val resultSetToEntityMapper: ResultSetToEntityMapper,
    private val executor: ExceptionLoggingSqlQueriesExecutor
) : EventStore {
    private val schema = "event_sourcing_store"
    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        executeQuery(
            QueryBuilder.insert(schema, EventRecordTable.name, EventRecordDto(eventRecord, aggregateTableName))
        )
    }

    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        executeQuery(
            QueryBuilder.batchInsert(schema, EventRecordTable.name, eventRecords.map { EventRecordDto(it, aggregateTableName) })
        )
    }

    override fun tableExists(aggregateTableName: String): Boolean {
        return true // ???
    }

    override fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot) {
        executeQuery(
            QueryBuilder.insertOrUpdateQuery(schema, SnapshotTable.name, SnapshotDto(snapshot, tableName))
        )
    }

    override fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: Any,
        aggregateVersion: Long
    ): List<EventRecord> {
        val query = QueryBuilder.select(schema, EventRecordTable.name)
            .andWhere("${EventRecordTable.aggregateId.name} = $aggregateId")
            .andWhere("${EventRecordTable.aggregateTableName.name} = $aggregateTableName")
            .andWhere("${EventRecordTable.aggregateVersion.name} > $aggregateVersion")
        val result = executeQueryReturningResultSet(query)
        return resultSetToEntityMapper.convertMany(result, EventRecord::class)
    }

    override fun findBatchOfEventRecordAfter(
        aggregateTableName: String,
        eventSequenceNum: Long,
        batchSize: Int
    ): List<EventRecord> {
        val query = QueryBuilder.select(schema, EventRecordTable.name)
            .andWhere("${EventRecordTable.aggregateTableName.name} = $aggregateTableName")
            .andWhere("${EventRecordTable.createdAt.name} > $eventSequenceNum")
        val result = executeQueryReturningResultSet(query)
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
        return executor.executeReturningBoolean {
            QueryBuilder.insertOrUpdateQuery(schema, EventStreamActiveReadersTable.name, ActiveEventStreamReaderDto(updatedActiveReader))
        }
    }

    override fun tryReplaceActiveStreamReader(
        expectedVersion: Long,
        newActiveReader: ActiveEventStreamReader
    ): Boolean {
       return executor.executeReturningBoolean {
           QueryBuilder.insertOrUpdateQuery(schema, EventStreamActiveReadersTable.name, ActiveEventStreamReaderDto(newActiveReader))
       }
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex): Boolean {
        return executor.executeReturningBoolean{
            QueryBuilder.insertOrUpdateQuery(schema, EventStreamReadIndexTable.name, EventStreamReadIndexDto(readIndex))
        }
    }

    private fun <T: Any> findEntityById(id: Any, clazz: KClass<T>) : T? {
        val (tableName, tableColumns) = when(clazz) {
            EventRecord::class -> EventRecordTable.name to EventRecordTable.columnNames()
            Snapshot::class -> SnapshotTable.name to SnapshotTable.columnNames()
            EventStreamReadIndex::class -> EventStreamReadIndexTable.name to EventStreamReadIndexTable.columnNames()
            EventStreamActiveReadersTable::class -> EventStreamActiveReadersTable.name to EventStreamActiveReadersTable.columnNames()
            else -> throw UnknownEntityException(clazz.simpleName)
        }
        val query = QueryBuilder.select(schema, tableName)
            .andWhere("$tableColumns = $id")
            .limit(1)

        val result = executeQueryReturningResultSet(query)
        return resultSetToEntityMapper.convert(result, clazz)
    }

    private fun executeQuery(query: Query) {
        val connection =  databaseConnectionFactory.getDatabaseConnection()
        executor.execute {
            query.execute(connection)
        }
    }

    private fun executeQueryReturningBoolean(query: Query) : Boolean {
        val connection =  databaseConnectionFactory.getDatabaseConnection()
        return executor.executeReturningBoolean {
            query.execute(connection)
        }
    }

    private fun executeQueryReturningResultSet(query: SelectQuery) : ResultSet? {
        val connection =  databaseConnectionFactory.getDatabaseConnection()
        return executor.executeReturningResultSet {
            query.execute(connection)
        }
    }
}