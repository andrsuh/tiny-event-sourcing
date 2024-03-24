package ru.quipy.query

import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import ru.quipy.exception.UnknownEntityClassException
import ru.quipy.query.exception.UnmappedDtoType
import ru.quipy.query.insert.BatchInsertQuery
import ru.quipy.query.insert.InsertQuery
import ru.quipy.query.insert.OnDuplicateKeyUpdateInsertQuery
import ru.quipy.query.select.SelectQuery
import ru.quipy.tables.ActiveEventStreamReaderDto
import ru.quipy.tables.Dto
import ru.quipy.tables.EventRecordDto
import ru.quipy.tables.EventRecordTable
import ru.quipy.tables.EventStreamActiveReadersTable
import ru.quipy.tables.EventStreamReadIndexDto
import ru.quipy.tables.EventStreamReadIndexTable
import ru.quipy.tables.SnapshotDto
import ru.quipy.tables.SnapshotTable
import kotlin.reflect.KClass

class QueryBuilder {
    companion object {
        fun <E: Dto> insert(schema: String, entity: E) : InsertQuery {
            return when(entity) {
                is EventRecordDto -> insertEventRecord(schema, entity)
                is SnapshotDto -> insertSnapshot(schema, entity)
                is ActiveEventStreamReaderDto -> insertActiveEventStreamReader(schema, entity)
                is EventStreamReadIndexDto -> insertEventStreamReadIndex(schema, entity)
                else ->  throw UnmappedDtoType(entity::class.simpleName)
            }
        }
        private fun insertEventRecord(schema: String, eventRecord: EventRecordDto) : InsertQuery {
            return InsertQuery(schema, EventRecordTable.name)
                .withColumns(columns = EventRecordTable.insertColumnNames())
                .withValues(values = eventRecord.values())
        }
        private fun insertSnapshot(schema: String, snapshot: SnapshotDto) : InsertQuery {
            return InsertQuery(schema, SnapshotTable.name)
                .withColumns(columns = SnapshotTable.insertColumnNames())
                .withValues(values = snapshot.values())
        }
        private fun insertActiveEventStreamReader(schema: String, activeStreamReader: ActiveEventStreamReaderDto) : InsertQuery {
            return InsertQuery(schema, EventStreamActiveReadersTable.name)
                .withColumns(columns = EventStreamActiveReadersTable.insertColumnNames())
                .withValues(values = activeStreamReader.values())
        }
        private fun insertEventStreamReadIndex(schema: String, eventStreamReadIndex: EventStreamReadIndexDto) : InsertQuery {
            return InsertQuery(schema, EventStreamReadIndexTable.name)
                .withColumns(columns = EventStreamReadIndexTable.insertColumnNames())
                .withValues(values = eventStreamReadIndex.values())
        }
        fun batchInsert(schema: String, relation: String, dtos: List<EventRecordDto>) : BatchInsertQuery {
            val query = BatchInsertQuery(schema, relation)
                .withColumns(columns = EventRecordTable.insertColumnNames())
            dtos.forEach { dto ->  query.withValues(values = dto.values())}
            return query
        }

         fun <E: Dto> insertOrUpdateWithLatestVersionQuery(schema: String, entity: E) : OnDuplicateKeyUpdateInsertQuery {
             return when(entity) {
                 is SnapshotDto -> insertOrUpdateSnapshotWithLatestVersionQuery(schema, entity)
                 is ActiveEventStreamReaderDto -> insertOrUpdateActiveStreamReaderWithLatestVersionQuery(schema, entity)
                 is EventStreamReadIndexDto -> insertOrUpdateStreamReaderWithLatestVersionQuery(schema, entity)
                 else -> throw UnmappedDtoType(entity::class.simpleName)
             }
         }

        fun <E: Dto> insertOrUpdateByIdAndVersionQuery(schema: String, id: Any, expectedVersion: Long, entity: E) : OnDuplicateKeyUpdateInsertQuery {
            return when(entity) {
                is SnapshotDto -> insertOrUpdateSnapshotByIdAndVersionQuery(schema, id, expectedVersion, entity)
                is ActiveEventStreamReaderDto -> insertOrUpdateActiveStreamReaderByIdAndVersionQuery(schema,id, expectedVersion, entity)
                is EventStreamReadIndexDto -> insertOrUpdateStreamReaderByIdAndVersionQuery(schema, id, expectedVersion, entity)
                else -> throw UnmappedDtoType(entity::class.simpleName)
            }
        }

        fun <T: Any> findEntityByIdQuery(schema: String, id: Any, clazz: KClass<T>) : SelectQuery {
            val (tableName, tableIdColumnName) = when (clazz) {
                EventRecord::class -> EventRecordTable.name to EventRecordTable.id.name
                Snapshot::class -> SnapshotTable.name to SnapshotTable.id.name
                EventStreamReadIndex::class -> EventStreamReadIndexTable.name to EventStreamReadIndexTable.id.name
                ActiveEventStreamReader::class -> EventStreamActiveReadersTable.name to EventStreamActiveReadersTable.id.name
                else -> throw UnknownEntityClassException(clazz.simpleName)
            }
            return select(schema, tableName)
                .andWhere("$tableName.$tableIdColumnName = '$id'")
                .limit(1)
        }

        private fun insertOrUpdateSnapshot(schema: String, entity: SnapshotDto) : OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, SnapshotTable.name)
                .withColumns(columns = SnapshotTable.insertColumnNames())
                .withValues(values = entity.values())
                .withPossiblyConflictingColumns(SnapshotTable.id.name)
                .onDuplicateKeyUpdateColumns(columns = SnapshotTable.onDuplicateKeyUpdateFields())
        }

        private fun insertOrUpdateActiveStreamReader(schema: String, entity: ActiveEventStreamReaderDto) : OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, EventStreamActiveReadersTable.name)
                .withColumns(columns = EventStreamActiveReadersTable.insertColumnNames())
                .withValues(values = entity.values())
                .withPossiblyConflictingColumns(EventStreamActiveReadersTable.id.name)
                .onDuplicateKeyUpdateColumns(columns = EventStreamActiveReadersTable.onDuplicateKeyUpdateFields())
        }

        private fun insertOrUpdateStreamReader(schema: String, entity: EventStreamReadIndexDto): OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, EventStreamReadIndexTable.name)
                .withColumns(columns = EventStreamReadIndexTable.insertColumnNames())
                .withValues(values = entity.values())
                .withPossiblyConflictingColumns(EventStreamReadIndexTable.id.name)
                .onDuplicateKeyUpdateColumns(columns = EventStreamReadIndexTable.onDuplicateKeyUpdateFields())
        }

        private fun insertOrUpdateSnapshotWithLatestVersionQuery(schema: String, entity: SnapshotDto) : OnDuplicateKeyUpdateInsertQuery {
            return insertOrUpdateSnapshot(schema, entity)
                .andWhere("${SnapshotTable.name}.${SnapshotTable.version.name} < ${entity.version}")
        }

        private fun insertOrUpdateActiveStreamReaderWithLatestVersionQuery(schema: String, entity: ActiveEventStreamReaderDto) : OnDuplicateKeyUpdateInsertQuery {
            return insertOrUpdateActiveStreamReader(schema, entity)
                .andWhere("${EventStreamActiveReadersTable.name}.${EventStreamActiveReadersTable.version.name} = ${entity.version - 1}")
        }

        private fun insertOrUpdateStreamReaderWithLatestVersionQuery(schema: String, entity: EventStreamReadIndexDto): OnDuplicateKeyUpdateInsertQuery {
            return insertOrUpdateStreamReader(schema, entity)
                .andWhere("${EventStreamReadIndexTable.name}.${EventStreamReadIndexTable.version.name} < ${entity.version}")
        }

        private fun insertOrUpdateSnapshotByIdAndVersionQuery(
            schema: String,
            id: Any,
            expectedVersion: Long,
            entity: SnapshotDto
        ): OnDuplicateKeyUpdateInsertQuery {
            return insertOrUpdateSnapshot(schema, entity)
                .andWhere("${SnapshotTable.name}.${SnapshotTable.id.name} = '$id'")
                .andWhere("${SnapshotTable.name}.${SnapshotTable.version.name} = $expectedVersion")
        }

        private fun insertOrUpdateStreamReaderByIdAndVersionQuery(
            schema: String,
            id: Any,
            expectedVersion: Long,
            entity: EventStreamReadIndexDto
        ): OnDuplicateKeyUpdateInsertQuery {
            return insertOrUpdateStreamReader(schema, entity)
                .andWhere("${EventStreamReadIndexTable.name}.${EventStreamReadIndexTable.id.name} = '$id'")
                .andWhere("${EventStreamReadIndexTable.name}.${EventStreamReadIndexTable.version.name} = $expectedVersion")
        }

        private fun insertOrUpdateActiveStreamReaderByIdAndVersionQuery(
            schema: String,
            id: Any,
            expectedVersion: Long,
            entity: ActiveEventStreamReaderDto
        ): OnDuplicateKeyUpdateInsertQuery {
            return insertOrUpdateActiveStreamReader(schema, entity)
                .andWhere("${EventStreamActiveReadersTable.name}.${EventStreamActiveReadersTable.id.name} = '$id'")
                .andWhere("${EventStreamActiveReadersTable.name}.${EventStreamActiveReadersTable.version.name} = $expectedVersion")
        }
        fun select(schema: String, relation: String) : SelectQuery {
            return SelectQuery(schema, relation)
        }
    }
}