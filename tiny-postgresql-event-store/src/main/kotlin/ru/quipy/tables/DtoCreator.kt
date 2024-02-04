package ru.quipy.tables

import ru.quipy.converter.EntityConverter
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot

class DtoCreator {
    companion object {
        fun <E> create(entity: E, tableName: String, entityConverter: EntityConverter) : Dto {
            return when (entity) {
                is EventRecord -> EventRecordDto(entity as EventRecord, tableName, entityConverter)
                is Snapshot -> SnapshotDto(entity as Snapshot, tableName, entityConverter)
                is EventStreamReadIndex -> EventStreamReadIndexDto(entity as EventStreamReadIndex)
                is ActiveEventStreamReader -> ActiveEventStreamReaderDto(entity as ActiveEventStreamReader)
                else -> throw IllegalStateException("No dto defined for entity ${entity!!::class.simpleName}")
            }
        }
    }
}