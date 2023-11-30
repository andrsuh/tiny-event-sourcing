package jp.veka.query

import jp.veka.query.exception.UnmappedDtoType
import jp.veka.query.insert.BatchInsertQuery
import jp.veka.query.insert.InsertQuery
import jp.veka.query.insert.OnDuplicateKeyUpdateInsertQuery
import jp.veka.query.select.SelectQuery
import jp.veka.tables.ActiveEventStreamReaderDto
import jp.veka.tables.Dto
import jp.veka.tables.EventRecordDto
import jp.veka.tables.EventRecordTable
import jp.veka.tables.EventStreamActiveReadersTable
import jp.veka.tables.EventStreamReadIndexDto
import jp.veka.tables.EventStreamReadIndexTable
import jp.veka.tables.SnapshotDto
import jp.veka.tables.SnapshotTable

class QueryBuilder {
    companion object {
        fun <E: Dto> insert(schema: String, relation: String, entity: E) : InsertQuery {
            return when(entity) {
                is EventRecordDto -> insertEventRecord(schema, relation, entity)
                is SnapshotDto -> insertSnapshot(schema, relation, entity)
                is ActiveEventStreamReaderDto -> insertActiveEventStreamReader(schema, relation, entity)
                is EventStreamReadIndexDto -> insertEventStreamReadIndex(schema, relation, entity)
                else ->  throw UnmappedDtoType(entity::class.simpleName)
            }
        }
        private fun insertEventRecord(schema: String, relation: String, eventRecord: EventRecordDto) : InsertQuery {
            return InsertQuery(schema, relation)
                .withColumns(columns = EventRecordTable.insertColumnNames())
                .withValues(values = eventRecord.values())
        }
        private fun insertSnapshot(schema: String, relation: String, snapshot: SnapshotDto) : InsertQuery {
            return InsertQuery(schema, relation)
                .withColumns(columns = SnapshotTable.insertColumnNames())
                .withValues(values = snapshot.values())
        }
        private fun insertActiveEventStreamReader(schema: String, relation: String, activeStreamReader: ActiveEventStreamReaderDto) : InsertQuery {
            return InsertQuery(schema, relation)
                .withColumns(columns = EventStreamActiveReadersTable.insertColumnNames())
                .withValues(values = activeStreamReader.values())
        }
        private fun insertEventStreamReadIndex(schema: String, relation: String, eventStreamReadIndex: EventStreamReadIndexDto) : InsertQuery {
            return InsertQuery(schema, relation)
                .withColumns(columns = EventStreamReadIndexTable.insertColumnNames())
                .withValues(values = eventStreamReadIndex.values())
        }
        fun batchInsert(schema: String, relation: String, dtos: List<EventRecordDto>) : BatchInsertQuery {
            val query = BatchInsertQuery(schema, relation, 1000)
                .withColumns(columns = EventRecordTable.insertColumnNames())
            dtos.forEach { dto ->  query.withValues(values = dto.values())}
            return query
        }

         fun <E: Dto> insertOrUpdateQuery(schema: String, relation: String, entity: E) : OnDuplicateKeyUpdateInsertQuery {
             return when(entity) {
                 is SnapshotDto -> insertOrUpdateSnapshotQuery(schema, relation, entity)
                 is ActiveEventStreamReaderDto -> insertOrUpdateActiveStreamReader(schema, relation, entity)
                 is EventStreamReadIndexDto -> insertOrUpdateStreamReader(schema, relation, entity)
                 else -> throw UnmappedDtoType(entity::class.simpleName)
             }
         }

        private fun insertOrUpdateSnapshotQuery(schema: String, relation: String, entity: SnapshotDto) : OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, relation)
                .withColumns(columns = SnapshotTable.insertColumnNames())
                .withValues(values = entity.values())
                .withPossiblyConflictingColumns(SnapshotTable.id.name)
                .onDuplicateKeyUpdateColumns(columns = SnapshotTable.onDuplicateKeyUpdateFields())
        }

        private fun insertOrUpdateActiveStreamReader(schema: String, relation: String, entity: ActiveEventStreamReaderDto) : OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, relation)
                .withColumns(columns = EventStreamActiveReadersTable.insertColumnNames())
                .withValues(values = entity.values())
                .withPossiblyConflictingColumns(EventStreamActiveReadersTable.id.name)
                .onDuplicateKeyUpdateColumns(columns = EventStreamActiveReadersTable.onDuplicateKeyUpdateFields())
        }

        private fun insertOrUpdateStreamReader(schema: String, relation: String, entity: EventStreamReadIndexDto): OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, relation)
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