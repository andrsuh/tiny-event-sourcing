package jp.veka.tables

import ru.quipy.saga.SagaContext

interface Table {
    fun name(): String
    fun idColumnName(): String
    fun versionColumnName(): String
    fun columnNames(): Array<String>
    fun onDuplicateKeyUpdateFields(): Array<String>
}
class EventRecordTable: Table {
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

    override fun idColumnName(): String {
        return id.name
    }

    override fun versionColumnName(): String {
        return aggregateVersion.name
    }

    override fun name(): String {
        return name
    }

    override fun columnNames(): Array<String> {
        return columnNames()
    }

    override fun onDuplicateKeyUpdateFields(): Array<String> {
        return arrayOf()
    }
}

class SnapshotTable: Table {
    companion object {
        const val name = "snapshot"
        val id = Column(1, "id", String::class)
        val snapshotTableName = Column(2, "snapshot_table_name", String::class)
        val snapshot = Column(3, "snapshot", String::class)
        val version = Column(4, "version", Long::class)

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
    override fun idColumnName(): String {
        return id.name
    }

    override fun versionColumnName(): String {
        return version.name
    }

    override fun name(): String {
        return name
    }
    override fun columnNames(): Array<String> {
        return columnNames()
    }

    override fun onDuplicateKeyUpdateFields(): Array<String> {
        return arrayOf(snapshot.name, version.name)
    }
}

class EventStreamReadIndexTable: Table {
    companion object {
        const val name = "event_stream_active_readers"
        val id = Column(1, "id", String::class)
        val readIndex = Column(2, "read_index", Long::class)
        val version = Column(3, "version", Long::class)

        fun columnNames(): Array<String> {
            return arrayOf(
                id.name,
                readIndex.name,
                version.name
            )
        }
    }
    override fun versionColumnName(): String {
        return version.name
    }
    override fun idColumnName(): String {
        return id.name
    }
    override fun name(): String {
        return EventRecordTable.name
    }
    override fun columnNames(): Array<String> {
        return columnNames()
    }

    override fun onDuplicateKeyUpdateFields(): Array<String> {
        return arrayOf(
            readIndex.name,
            version.name
        )
    }
}

class EventStreamActiveReadersTable: Table {
    companion object {
        const val name = "event_stream_read_index"
        val id = Column(1, "id", String::class)
        val version = Column(2, "version", Long::class)
        val readerId = Column(3, "reader_id", String::class)
        val readPosition = Column(4, "read_position", Long::class)
        val lastInteraction = Column(5, "last_interaction", Long::class)

        fun columnNames(): Array<String> {
            return arrayOf(
                id.name,
                version.name,
                readerId.name,
                readPosition.name,
                lastInteraction.name
            )
        }
    }
    override fun idColumnName(): String {
        return id.name
    }
    override fun versionColumnName(): String {
        return version.name
    }
    override fun name(): String {
        return name
    }
    override fun columnNames(): Array<String> {
        return columnNames()
    }

    override fun onDuplicateKeyUpdateFields(): Array<String> {
        return arrayOf(
            version.name,
            readerId.name,
            readPosition.name,
            lastInteraction.name
        )
    }
}