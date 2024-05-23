package ru.quipy

import org.apache.logging.log4j.LogManager
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.annotation.Transactional
import ru.quipy.converter.EntityConverter
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import ru.quipy.domain.Unique
import ru.quipy.domain.Versioned
import ru.quipy.mappers.MapperFactory
import ru.quipy.query.Query
import ru.quipy.query.QueryBuilder
import ru.quipy.query.select.SelectQuery
import ru.quipy.saga.SagaContext
import ru.quipy.tables.ActiveEventStreamReaderDto
import ru.quipy.tables.DtoCreator
import ru.quipy.tables.EventRecordDto
import ru.quipy.tables.EventRecordTable
import ru.quipy.tables.EventStreamActiveReadersTable
import ru.quipy.tables.EventStreamReadIndexDto
import ru.quipy.tables.SnapshotDto
import java.sql.PreparedStatement
import java.sql.SQLException
import kotlin.reflect.KClass

open class PostgresTemplateEventStore(
    private val jdbcTemplate: JdbcTemplate,
    private val eventStoreSchemaName: String,
    private val mapperFactory: MapperFactory,
    private val entityConverter: EntityConverter) : EventStore {
    companion object {
        private val logger = LogManager.getLogger(PostgresTemplateEventStore::class)
    }
    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        try {
            jdbcTemplate.execute(
                QueryBuilder.insert(
                    eventStoreSchemaName,
                    EventRecordDto(eventRecord, aggregateTableName, entityConverter)
                ).build()
            )
        } catch (e : DuplicateKeyException) {
            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventRecord", e)
        }
    }

    @Transactional
    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        val template =  QueryBuilder.batchInsert(eventStoreSchemaName,
            EventRecordTable.name,
            eventRecords.map { EventRecordDto(it, aggregateTableName, entityConverter) }
        ).getTemplate()
        try {
            jdbcTemplate.batchUpdate(template, object : BatchPreparedStatementSetter {
                @Throws(SQLException::class)
                override fun setValues(preparedStatement: PreparedStatement, i: Int) {
                    val item = eventRecords[i]
                    preparedStatement.setString(EventRecordTable.id.index, item.id)
                    preparedStatement.setString(EventRecordTable.aggregateTableName.index, aggregateTableName)
                    preparedStatement.setString(EventRecordTable.aggregateId.index, item.aggregateId.toString())
                    preparedStatement.setLong(EventRecordTable.aggregateVersion.index, item.aggregateVersion)
                    preparedStatement.setString(EventRecordTable.eventTitle.index, item.eventTitle)
                    preparedStatement.setString(EventRecordTable.payload.index, item.payload)
                    preparedStatement.setString(
                        EventRecordTable.sagaContext.index,
                        item.sagaContext?.let {  entityConverter.serialize(it) } ?: "null"
                    )
                }

                override fun getBatchSize(): Int {
                    return eventRecords.size
                }
            })
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
        jdbcTemplate.execute(
            QueryBuilder.insertOrUpdateWithLatestVersionQuery(
                eventStoreSchemaName,
                SnapshotDto(snapshot, tableName, entityConverter)
            ).build()
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
        return jdbcTemplate.query(query.build(), mapperFactory.getMapper(EventRecord::class))
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
        return jdbcTemplate.query(query.build(), mapperFactory.getMapper(EventRecord::class))
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
        return executeQueryReturningBoolean(
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
        return executeQueryReturningBoolean(
            QueryBuilder.insertOrUpdateWithLatestVersionQuery(eventStoreSchemaName, EventStreamReadIndexDto(readIndex))
        )
    }

    private fun <T: Any> findEntityById(id: Any, clazz: KClass<T>) : T? {
        return jdbcTemplate.query(QueryBuilder.findEntityByIdQuery(
            eventStoreSchemaName, id, clazz).build(),
            mapperFactory.getMapper(clazz)
        ).firstOrNull()
    }

    private fun executeQueryReturningBoolean(query: Query) : Boolean{
        return try {
            jdbcTemplate.execute(query.build())
            true
        } catch (e : Exception) {
            logger.error(e.stackTrace)
            false
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
                executeQueryReturningBoolean(query)
            } else {
                try {
                    val query = QueryBuilder.insert(eventStoreSchemaName, DtoCreator.create(entity, tableName, entityConverter))
                    jdbcTemplate.execute(query.build())
                    true
                } catch (e: DuplicateKeyException) {
                    logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
                    continue
                }
            }
        }
    }
}