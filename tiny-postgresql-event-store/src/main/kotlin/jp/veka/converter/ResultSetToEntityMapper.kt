package jp.veka.converter

import jp.veka.converter.exception.NoMapperForClass
import jp.veka.tables.EventRecordTable
import jp.veka.tables.EventStreamActiveReadersTable
import jp.veka.tables.EventStreamReadIndexTable
import jp.veka.tables.SnapshotTable
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.AggregateState
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import ru.quipy.saga.SagaContext
import java.sql.ResultSet
import kotlin.reflect.KClass

@Suppress("UNCHECKED_CAST")
class ResultSetToEntityMapper(private val entityConverter: EntityConverter) {
    public fun <T : Any> convert(resultSet: ResultSet?, clazz: KClass<T>) : T? {
        resultSet ?: return null
        if (!resultSet.next()) return null
        return when(clazz) {
            EventRecord::class -> mapToEventRecord(resultSet)
            Snapshot::class -> mapToSnapshot(resultSet)
            EventStreamReadIndex::class -> mapToEventStreamReader(resultSet)
            ActiveEventStreamReader::class -> mapToActiveEventStreamReader(resultSet)
            else -> throw NoMapperForClass(clazz.simpleName)
        } as T
    }

    public fun <T : Any> convertMany(resultSet: ResultSet?, clazz: KClass<T>) : List<T> {
        resultSet ?: return  listOf()
        val result = mutableListOf<T>()
        while (resultSet.next()) {
            val res = convert(resultSet, clazz)
            if (res != null) result.add(res)
        }

        return result
    }
    private fun mapToEventRecord(resultSet: ResultSet): EventRecord {
        return EventRecord(
            resultSet.getString(EventRecordTable.id.index),
            resultSet.getString(EventRecordTable.aggregateId.index),
            resultSet.getLong(EventRecordTable.aggregateVersion.index),
            resultSet.getString(EventRecordTable.eventTitle.index),
            resultSet.getString(EventRecordTable.payload.index),
            entityConverter.toObject(resultSet.getString(EventRecordTable.sagaContext.index), SagaContext::class),
            resultSet.getLong(EventRecordTable.createdAt.index)
        )
    }

    private fun mapToSnapshot(resultSet: ResultSet) : Snapshot {
        return Snapshot(
            resultSet.getString(SnapshotTable.id.index),
            entityConverter.toObject(resultSet.getString(SnapshotTable.snapshot.index), AggregateState::class),
            resultSet.getLong(SnapshotTable.version.index)
        )
    }

    private fun mapToEventStreamReader(resultSet: ResultSet) : EventStreamReadIndex {
        return EventStreamReadIndex(
            resultSet.getString(EventStreamReadIndexTable.id.index),
            resultSet.getLong(EventStreamReadIndexTable.readIndex.index),
            resultSet.getLong(EventStreamReadIndexTable.version.index)
        )
    }

    private fun mapToActiveEventStreamReader(resultSet: ResultSet) : ActiveEventStreamReader {
        return ActiveEventStreamReader(
            resultSet.getString(EventStreamActiveReadersTable.id.index),
            resultSet.getLong(EventStreamActiveReadersTable.version.index),
            resultSet.getString(EventStreamActiveReadersTable.readerId.index),
            resultSet.getLong(EventStreamActiveReadersTable.readPosition.index),
            resultSet.getLong(EventStreamActiveReadersTable.lastInteraction.index)
        )
    }
}