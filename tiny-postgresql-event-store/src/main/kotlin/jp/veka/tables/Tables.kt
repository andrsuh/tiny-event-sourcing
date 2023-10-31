package jp.veka.tables

import ru.quipy.saga.SagaContext
class EventRecordTable {
    companion object {
        const val name = "event_record"
        val id = Column(1, "id", String::class.java)
        val aggregateTableName = Column(2, "aggregate_table_name", String::class.java)
        val aggregateId = Column(3, "aggregate_id", String::class.java)
        val aggregateVersion = Column(4, "aggregate_version", Long::class.java)
        val eventTitle = Column(5, "event_title", String::class.java)
        val payload = Column(6, "payload", String::class.java)
        val sagaContext = Column(7, "saga_context", SagaContext::class.java)
        val createdAt = Column(8, "createdAt", Long::class.java)
        fun columnNames(): Array<String> {
            return arrayOf(id.name,
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
        val id = Column(1, "id", String::class.java)
        val snapshotTableName = Column(2, "snapshot_table_name", String::class.java)
        val snapshot = Column(3, "snapshot", String::class.java)
        val version = Column(4, "version", Long::class.java)

        fun columnNames(): Array<String> {
            return arrayOf(
                id.name,
                snapshotTableName.name,
                snapshot.name,
                version.name
            )
        }

        fun onDuplicateKeyUpdateFields(): Array<String> {
            return arrayOf(snapshot.name, version.name)
        }
    }
}

class EventStreamReadIndexTable {
    companion object {
        const val name = "event_stream_active_readers"
        val id = Column(1, "id", String::class.java)
        val readIndex = Column(2, "read_index", Long::class.java)
        val version = Column(3, "version", Long::class.java)

        fun columnNames(): Array<String> {
            return arrayOf(
                id.name,
                readIndex.name,
                version.name
            )
        }
        fun onDuplicateKeyUpdateFields(): Array<String> {
            return arrayOf(
                readIndex.name,
                version.name
            )
        }
    }
}

class EventStreamActiveReadersTable {
    companion object {
        const val name = "event_stream_read_index"
        val id = Column(1, "id", String::class.java)
        val version = Column(2, "version", Long::class.java)
        val readerId = Column(3, "reader_id", String::class.java)
        val readPosition = Column(4, "read_position", Long::class.java)
        val lastInteraction = Column(5, "last_interaction", Long::class.java)

        fun columnNames(): Array<String> {
            return arrayOf(
                id.name,
                version.name,
                readerId.name,
                readPosition.name,
                lastInteraction.name
            )
        }
        fun onDuplicateKeyUpdateFields(): Array<String> {
            return arrayOf(
                version.name,
                readerId.name,
                readPosition.name,
                lastInteraction.name
            )
        }
    }
}