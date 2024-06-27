package ru.quipy

import org.apache.logging.log4j.LogManager
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.SqlOutParameter
import org.springframework.jdbc.core.SqlParameter
import org.springframework.transaction.annotation.Transactional
import ru.quipy.converter.EntityConverter
import ru.quipy.core.BatchMode.JDBC_BATCH
import ru.quipy.core.BatchMode.STORED_PROCEDURE
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStore
import ru.quipy.domain.*
import ru.quipy.mappers.MapperFactory
import ru.quipy.query.Query
import ru.quipy.query.QueryBuilder
import ru.quipy.query.select.SelectQuery
import ru.quipy.tables.*
import ru.quipy.utils.Batcher
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Types
import kotlin.reflect.KClass
import kotlin.time.Duration.Companion.milliseconds


open class PostgresTemplateEventStore(
    private val jdbcTemplate: JdbcTemplate,
    private val eventStoreSchemaName: String,
    private val mapperFactory: MapperFactory,
    private val entityConverter: EntityConverter,
    private val props: EventSourcingProperties,
) : EventStore {

    private val batcher: Batcher = Batcher(props.batchSize, (props.batchPeriodMillis).milliseconds) { statements ->
        when(props.batchMode) {
            JDBC_BATCH.name -> jdbcTemplate.batchUpdate(*(statements.toTypedArray())).toTypedArray()
            STORED_PROCEDURE.name -> executeSqlCommands(statements)
            else -> throw IllegalArgumentException("Unsupported batch mode: ${props.batchMode}")
        }
    }
    companion object {
        private val logger = LogManager.getLogger(PostgresTemplateEventStore::class)
        const val PROCEDURE_NAME: String = "execute_batch"
    }

    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        try {
            val statement = QueryBuilder.insert(
                eventStoreSchemaName,
                EventRecordDto(eventRecord, aggregateTableName, entityConverter)
            ).build()

            if (props.batchEnabled) {
                val res = batcher.delayedExecution(eventRecord.id, statement).get()
                if (!res) {
                    logger.warn("Failed to insert event within batch. Retrying: $statement")
                    throw DuplicateEventIdException("Batch returned error for: $eventRecord", null)
                }
            } else {
                jdbcTemplate.execute(statement)
            }
        } catch (e: DuplicateKeyException) {
            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventRecord", e)
        }
    }

    private fun executeSqlCommands(commands: List<String>): Array<Int> {
        val result = jdbcTemplate.call(
            { con ->
                val cs = con.prepareCall("{? = call $PROCEDURE_NAME(?)}")
                cs.registerOutParameter(1, Types.ARRAY)
                cs.setArray(2, con.createArrayOf("text", commands.toTypedArray()))
                cs
            },
            listOf<SqlParameter>(
                SqlOutParameter("return", Types.ARRAY)
            )
        )

        val sqlArray = result["return"] as java.sql.Array
        val output = sqlArray.array as Array<Int>
        return output
    }

    @Transactional
    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        val template = QueryBuilder.batchInsert(eventStoreSchemaName,
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
        } catch (e:  DuplicateKeyException) {
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

    private fun executeQueryReturningBoolean(query: Query): Boolean {
        return try {
            jdbcTemplate.execute(query.build())
            true
        } catch (e: Exception) {
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