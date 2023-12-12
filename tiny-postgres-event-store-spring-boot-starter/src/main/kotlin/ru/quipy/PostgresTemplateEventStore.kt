package ru.quipy

import ru.quipy.converter.EntityConverter
import ru.quipy.mappers.MapperFactory
import ru.quipy.query.Query
import ru.quipy.query.QueryBuilder
import ru.quipy.tables.ActiveEventStreamReaderDto
import ru.quipy.tables.EventRecordDto
import ru.quipy.tables.EventRecordTable
import ru.quipy.tables.EventStreamActiveReadersTable
import ru.quipy.tables.EventStreamReadIndexDto
import ru.quipy.tables.SnapshotDto
import org.apache.logging.log4j.LogManager
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import ru.quipy.saga.SagaContext
import java.sql.PreparedStatement
import java.sql.SQLException
import kotlin.reflect.KClass

class PostgresTemplateEventStore(
    private val jdbcTemplate: JdbcTemplate,
    private val eventStoreSchemaName: String,
    private val mapperFactory: MapperFactory,
    private val batchInsertSize: Int,
    private val entityConverter: EntityConverter) : EventStore {
    companion object {
        private val logger = LogManager.getLogger(ru.quipy.PostgresTemplateEventStore::class)
    }
    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        jdbcTemplate.execute(
            QueryBuilder.insert(
                eventStoreSchemaName,
                EventRecordDto(eventRecord, aggregateTableName, entityConverter)
            ).build()
        )
    }

    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        val template =  QueryBuilder.batchInsert(eventStoreSchemaName,
            EventRecordTable.name,
            eventRecords.map { EventRecordDto(it, aggregateTableName, entityConverter) }
        ).getTemplate()
        val batches = eventRecords.chunked(batchInsertSize)
        for (batch in batches) {
            jdbcTemplate.batchUpdate(template, object : BatchPreparedStatementSetter {
                @Throws(SQLException::class)
                override fun setValues(preparedStatement: PreparedStatement, i: Int) {
                    val item = batch[i]
                    preparedStatement.setString(EventRecordTable.id.index, item.id)
                    preparedStatement.setString(EventRecordTable.aggregateTableName.index, aggregateTableName)
                    preparedStatement.setString(EventRecordTable.aggregateId.index, item.aggregateId.toString())
                    preparedStatement.setLong(EventRecordTable.aggregateVersion.index, item.aggregateVersion)
                    preparedStatement.setLong(EventRecordTable.eventTitle.index, item.aggregateVersion)
                    preparedStatement.setString(EventRecordTable.payload.index, item.payload)
                    preparedStatement.setString(
                        EventRecordTable.sagaContext.index,
                        entityConverter.serialize(item.sagaContext ?: SagaContext())
                    )
                    preparedStatement.setLong(EventRecordTable.createdAt.index, item.createdAt)
                }

                override fun getBatchSize(): Int {
                    return batch.size
                }
            })
        }
    }

    override fun tableExists(aggregateTableName: String): Boolean {
        return true
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
            .andWhere("${EventRecordTable.createdAt.name} > $eventSequenceNum") // id > $eventSequenceNum ??
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
            QueryBuilder.insertOrUpdateWithLatestVersionQuery(eventStoreSchemaName, ActiveEventStreamReaderDto(updatedActiveReader))
        )
    }

    override fun tryReplaceActiveStreamReader(
        expectedVersion: Long,
        newActiveReader: ActiveEventStreamReader
    ): Boolean {
        return executeQueryReturningBoolean(
            QueryBuilder.insertOrUpdateWithLatestVersionQuery(eventStoreSchemaName, ActiveEventStreamReaderDto(newActiveReader))
                .andWhere("${EventStreamActiveReadersTable.name}.${EventStreamActiveReadersTable.version.name} = $expectedVersion")
        )
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
            ru.quipy.PostgresTemplateEventStore.Companion.logger.error(e.stackTrace)
            false
        }
    }
}