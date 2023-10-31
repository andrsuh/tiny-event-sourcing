package jp.veka.query

import jp.veka.query.delete.DeleteQuery
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
                else ->  throw UnmappedDtoType(entity::class.simpleName)
            }
        }
        private fun insertEventRecord(schema: String, relation: String, eventRecord: EventRecordDto) : InsertQuery {
            return InsertQuery(schema, relation)
                .withColumns(columns = EventRecordTable.columnNames())
                .withValues(eventRecord.values())
        }
        private fun insertSnapshot(schema: String, relation: String, snapshot: SnapshotDto) : InsertQuery {
            return InsertQuery(schema, relation)
                .withColumns(columns = EventRecordTable.columnNames())
                .withValues(snapshot.values())
        }

        fun batchInsert(schema: String, relation: String, dtos: List<EventRecordDto>) : BatchInsertQuery {
            val query = BatchInsertQuery(schema, relation, 100)
                .withColumns(columns = EventRecordTable.columnNames())
            dtos.forEach { dto ->  query.withValues(dto.values())}
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
                .withColumns(columns = SnapshotTable.columnNames())
                .withValues(entity.values())
                .onDuplicateKeyUpdateValues(columns = SnapshotTable.onDuplicateKeyUpdateFields())
        }

        private fun insertOrUpdateActiveStreamReader(schema: String, relation: String, entity: ActiveEventStreamReaderDto) : OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, relation)
                .withColumns(columns = EventStreamActiveReadersTable.columnNames())
                .withValues(entity.values())
                .onDuplicateKeyUpdateValues(columns = EventStreamActiveReadersTable.onDuplicateKeyUpdateFields())
        }

        private fun insertOrUpdateStreamReader(schema: String, relation: String, entity: EventStreamReadIndexDto): OnDuplicateKeyUpdateInsertQuery {
            return OnDuplicateKeyUpdateInsertQuery(schema, relation)
                .withColumns(columns = EventStreamReadIndexTable.columnNames())
                .withValues(entity.values())
                .onDuplicateKeyUpdateValues(columns = EventStreamReadIndexTable.onDuplicateKeyUpdateFields())
        }
        fun select(schema: String, relation: String) : SelectQuery {
            return SelectQuery(schema, relation)
        }

        fun delete(schema: String, relation: String) : DeleteQuery {
            return DeleteQuery(schema, relation)
        }
    }
}