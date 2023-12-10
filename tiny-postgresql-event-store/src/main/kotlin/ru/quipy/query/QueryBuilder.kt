package ru.quipy.query

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
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
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
        private fun insertSnapshot(schema: String,snapshot: SnapshotDto) : InsertQuery {
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
            val query = BatchInsertQuery(schema, relation, 1000)
                .withColumns(columns = EventRecordTable.insertColumnNames())
            dtos.forEach { dto ->  query.withValues(values = dto.values())}
            return query
        }

         fun <E: Dto> insertOrUpdateQuery(schema: String, entity: E) : OnDuplicateKeyUpdateInsertQuery {
             return when(entity) {
                 is SnapshotDto -> insertOrUpdateSnapshotQuery(schema, entity)
                 is ActiveEventStreamReaderDto -> insertOrUpdateActiveStreamReader(schema, entity)
                 is EventStreamReadIndexDto -> insertOrUpdateStreamReader(schema, entity)
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
                .andWhere("$tableIdColumnName = '$id'")
                .limit(1)
        }

        private fun insertOrUpdateSnapshotQuery(schema: String, entity: SnapshotDto) : OnDuplicateKeyUpdateInsertQuery {
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
        fun select(schema: String, relation: String) : SelectQuery {
            return SelectQuery(schema, relation)
        }
    }
}