package ru.quipy

import org.postgresql.util.PSQLException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.transaction.annotation.Transactional
import ru.quipy.converter.EntityConverter
import ru.quipy.converter.ResultSetToEntityMapper
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import ru.quipy.domain.Unique
import ru.quipy.domain.Versioned
import ru.quipy.executor.QueryExecutor
import ru.quipy.query.QueryBuilder
import ru.quipy.query.select.SelectQuery
import ru.quipy.tables.ActiveEventStreamReaderDto
import ru.quipy.tables.DtoCreator
import ru.quipy.tables.EventRecordDto
import ru.quipy.tables.EventRecordTable
import ru.quipy.tables.EventStreamActiveReadersTable
import ru.quipy.tables.EventStreamReadIndexDto
import ru.quipy.tables.SnapshotDto
import java.sql.ResultSet
import kotlin.reflect.KClass

open class PostgresClientEventStore(
    private val eventStoreSchemaName: String,
    private val resultSetToEntityMapper: ResultSetToEntityMapper,
    private val entityConverter: EntityConverter,
    private val executor: QueryExecutor
) : EventStore {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(PostgresClientEventStore::class.java)
    }
    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        try {
            executor.execute(
                QueryBuilder.insert(
                    eventStoreSchemaName,
                    EventRecordDto(eventRecord, aggregateTableName, entityConverter))
            )
        } catch (e : DuplicateKeyException) {
            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventRecord", e)
        }

    }

    @Transactional
    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        try {
            executor.execute(
                QueryBuilder.batchInsert(eventStoreSchemaName, EventRecordTable.name, eventRecords.map { EventRecordDto(it, aggregateTableName, entityConverter) })
            )
        } catch (e :  DuplicateKeyException) {
            throw DuplicateEventIdException(
                "There is record with such an id. Record set cannot be saved $eventRecords",
                e
            )
        }
    }

    override fun tableExists(aggregateTableName: String): Boolean {
        return true // TODO partition?
    }

    override fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot) {
        executor.execute(
            QueryBuilder.insertOrUpdateWithLatestVersionQuery(
                eventStoreSchemaName,
                SnapshotDto(snapshot, tableName, entityConverter))
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
        return executor.executeAndProcessResultSet(query) {
            resultSetToEntityMapper.convertMany(it, EventRecord::class)
        } ?: listOf()
    }

    override fun findBatchOfEventRecordAfter(
        aggregateTableName: String,
        eventSequenceNum: Long,
        batchSize: Int
    ): List<EventRecord> {
        val query = QueryBuilder.select(eventStoreSchemaName, EventRecordTable.name)
            .andWhere("${EventRecordTable.aggregateTableName.name} = '$aggregateTableName'")
            .andWhere("${EventRecordTable.createdAt.name} > $eventSequenceNum")
            .orderBy(EventRecordTable.createdAt.name, SelectQuery.SortingOrder.ASCENDING)
            .limit(batchSize)
        return executor.executeAndProcessResultSet(query) {
            resultSetToEntityMapper.convertMany(it, EventRecord::class)
        } ?: listOf()
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
            QueryBuilder.insertOrUpdateByIdAndVersionQuery(eventStoreSchemaName, updatedActiveReader.id, updatedActiveReader.version - 1, ActiveEventStreamReaderDto(updatedActiveReader))
        )
    }

    override fun tryReplaceActiveStreamReader(
        expectedVersion: Long,
        newActiveReader: ActiveEventStreamReader
    ): Boolean {
       return tryReplaceWithOptimisticLock(EventStreamActiveReadersTable.name, expectedVersion, newActiveReader)
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex): Boolean {
        return executor.executeReturningBoolean(
            QueryBuilder.insertOrUpdateWithLatestVersionQuery(eventStoreSchemaName, EventStreamReadIndexDto(readIndex))
        )
    }

    private fun <T: Any> findEntityById(id: Any, clazz: KClass<T>) : T? {
        val query = QueryBuilder.findEntityByIdQuery(eventStoreSchemaName, id, clazz)
        return executor.executeAndProcessResultSet(query) { rs: ResultSet? ->
            resultSetToEntityMapper.convert(rs, clazz)
        }
    }

    private fun <E> tryReplaceWithOptimisticLock(
        tableName: String,
        expectedVersion: Long,
        entity: E
    ): Boolean where E : Versioned, E : Unique<*> {
        while (true) {
            val existingEntity = findEntityById(entity.id!!, entity::class)
            return if (existingEntity != null) {
                val query = QueryBuilder.insertOrUpdateByIdAndVersionQuery(eventStoreSchemaName,
                    entity.id!!, expectedVersion, DtoCreator.create(entity, tableName, entityConverter))
                executor.executeReturningBoolean(query)
            } else {
                try {
                    val query = QueryBuilder.insert(eventStoreSchemaName, DtoCreator.create(entity, tableName, entityConverter))
                    executor.execute(query)
                    true
                } catch (e: PSQLException) {
                    logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
                    continue
                }
            }
        }
    }
}