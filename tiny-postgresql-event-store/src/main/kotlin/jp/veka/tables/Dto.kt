package jp.veka.tables

import jp.veka.converter.JsonEntityConverter
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import ru.quipy.saga.SagaContext

var entityConverter = JsonEntityConverter()
interface Dto {
    fun values() : Array<Any>
}
class EventRecordDto(
    val id: String,
    val aggregateTableName: String,
    val aggregateId: String,
    val aggregateVersion: Long,
    val eventTitle: String,
    val payload: String,
    var sagaContext: String,
    val createdAt: Long = System.currentTimeMillis()
) : Dto {
    constructor(eventRecord: EventRecord, aggregateTableName: String)
    : this(eventRecord.id,
        aggregateTableName,
        eventRecord.aggregateId.toString(),
        eventRecord.aggregateVersion,
        eventRecord.eventTitle,
        eventRecord.payload,
        entityConverter.serialize(eventRecord.sagaContext?: SagaContext()),
        eventRecord.createdAt)

    override fun values(): Array<Any> {
        return arrayOf(
            id, aggregateTableName, aggregateId, aggregateVersion, eventTitle, payload, sagaContext, createdAt
        )
    }
}

class SnapshotDto(
    val id : String,
    val snapshotTableName: String,
    val snapshot : String,
    var version: Long
) : Dto {
    constructor(snapshot: Snapshot, snapshotTableName: String)
        : this(snapshot.id.toString(),
            snapshotTableName,
            entityConverter.serialize(snapshot.snapshot),
            snapshot.version)

    override fun values(): Array<Any> {
        return arrayOf(id, snapshotTableName, snapshot, version)
    }
}


class EventStreamReadIndexDto(
    val id: String,
    val readIndex: Long,
    var version: Long
) : Dto {
    constructor(eventStreamReadIndex: EventStreamReadIndex)
    : this(eventStreamReadIndex.id,
        eventStreamReadIndex.readIndex,
        eventStreamReadIndex.version)

    override fun values(): Array<Any> {
        return arrayOf(id, readIndex, version)
    }
}

class ActiveEventStreamReaderDto(
    val id: String,
    var version: Long,
    val readerId: String,
    val readPosition: Long,
    val lastInteraction: Long
) : Dto {
    constructor(activeEventStreamReader: ActiveEventStreamReader)
    : this(activeEventStreamReader.id,
        activeEventStreamReader.version,
        activeEventStreamReader.readerId,
        activeEventStreamReader.readPosition,
        activeEventStreamReader.lastInteraction)

    override fun values(): Array<Any> {
        return arrayOf(id, version, readerId, readPosition, lastInteraction)
    }
}