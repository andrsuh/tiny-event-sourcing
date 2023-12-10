package ru.quipy.tables

import ru.quipy.saga.SagaContext

class EventRecordTable {
    companion object {
        const val name = "event_record"
        val id = Column(1, "id", String::class)
        val aggregateTableName = Column(2, "aggregate_table_name", String::class)
        val aggregateId = Column(3, "aggregate_id", String::class)
        val aggregateVersion = Column(4, "aggregate_version", Long::class)
        val eventTitle = Column(5, "event_title", String::class)
        val payload = Column(6, "payload", String::class)
        val sagaContext = Column(7, "saga_context", SagaContext::class)
        val createdAt = Column(8, "createdAt", Long::class)
        fun insertColumnNames() : Array<String> {
            return arrayOf(aggregateTableName.name,
                aggregateId.name,
                aggregateVersion.name,
                eventTitle.name,
                payload.name,
                sagaContext.name,
                createdAt.name)
        }

        fun allColumnNames() : Array<String> {
            return arrayOf(
                id.name,
                aggregateTableName.name,
                aggregateId.name,
                aggregateVersion.name,
                eventTitle.name,
                payload.name,
                sagaContext.name,
                createdAt.name)
        }
    }
}

class SnapshotTable {
    companion object {
        const val name = "snapshot"
        val id = Column(1, "id", String::class)
        val snapshotTableName = Column(2, "snapshot_table_name", String::class)
        val snapshot = Column(3, "snapshot", String::class)
        val version = Column(4, "version", Long::class)

        fun insertColumnNames() : Array<String> {
            return arrayOf(
                id.name,
                snapshotTableName.name,
                snapshot.name,
                version.name
            )
        }
        fun allColumnNames() : Array<String> {
            return arrayOf(
                id.name,
                snapshotTableName.name,
                snapshot.name,
                version.name
            )
        }

        fun onDuplicateKeyUpdateFields() : Array<String> {
            return arrayOf(snapshot.name, version.name)
        }
    }
}

class EventStreamReadIndexTable {
    companion object {
        const val name = "event_stream_read_index"
        val id = Column(1, "id", String::class)
        val readIndex = Column(2, "read_index", Long::class)
        val version = Column(3, "version", Long::class)

        fun insertColumnNames() : Array<String> {
            return arrayOf(
                id.name,
                readIndex.name,
                version.name
            )
        }

        fun allColumnNames() : Array<String> {
            return arrayOf(
                id.name,
                readIndex.name,
                version.name
            )
        }
        fun onDuplicateKeyUpdateFields() : Array<String> {
            return arrayOf(
                readIndex.name,
                version.name
            )
        }
    }
}

class EventStreamActiveReadersTable {
    companion object {
        const val name = "event_stream_active_readers"
        val id = Column(1, "id", String::class)
        val version = Column(2, "version", Long::class)
        val readerId = Column(3, "reader_id", String::class)
        val readPosition = Column(4, "read_position", Long::class)
        val lastInteraction = Column(5, "last_interaction", Long::class)

        fun insertColumnNames() : Array<String> {
            return arrayOf(
                id.name,
                version.name,
                readerId.name,
                readPosition.name,
                lastInteraction.name
            )
        }
        fun allColumnNames() : Array<String> {
            return arrayOf(
                id.name,
                version.name,
                readerId.name,
                readPosition.name,
                lastInteraction.name
            )
        }
        fun onDuplicateKeyUpdateFields() : Array<String> {
            return arrayOf(
                version.name,
                readerId.name,
                readPosition.name,
                lastInteraction.name
            )
        }
    }
}