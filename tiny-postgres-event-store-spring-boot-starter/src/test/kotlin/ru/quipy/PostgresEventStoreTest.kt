package ru.quipy

import ru.quipy.config.TestDbConfig
import ru.quipy.converter.EntityConverter
import ru.quipy.exception.UnknownEntityClassException
import ru.quipy.executor.QueryExecutor
import ru.quipy.query.QueryBuilder
import ru.quipy.tables.ActiveEventStreamReaderDto
import ru.quipy.tables.EventRecordDto
import ru.quipy.tables.EventRecordTable
import ru.quipy.tables.EventStreamActiveReadersTable
import ru.quipy.tables.EventStreamReadIndexDto
import ru.quipy.tables.EventStreamReadIndexTable
import ru.quipy.tables.SnapshotDto
import ru.quipy.tables.SnapshotTable
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import ru.quipy.autoconfigure.PostgresEventStoreAutoConfiguration
import ru.quipy.config.FlywayConfig
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import ru.quipy.saga.SagaContext

@SpringBootTest(
    classes = [
        TestDbConfig::class,
        FlywayConfig::class,
        PostgresEventStoreAutoConfiguration::class
    ]
)
class PostgresEventStoreTest {
    companion object {
        private const val aggregateTableName = "test_aggregate_table"
        private const val snapshotsTableName = "snapshots_table_name"
        private const val timestamp0 = 0L
        private const val timestamp1 = 1L
        private const val timestamp2 = 2L
        private const val timestamp3 = 3L
        private const val aggregateId1 = "1"
        private const val aggregateId2 = "2"
        private const val aggregateId3 = "3"
        private const val aggregateVersion0 = 0L
        private const val aggregateVersion1 = 1L
        private const val aggregateVersion2 = 2L
        private const val aggregateVersion3 = 3L
        private const val snapshotId1 = 1L
        private const val snapshotId2 = 2L
        private const val streamName = "streamName"
        private const val otherStreamName = "otherStreamName"
        private const val snapshotVersion0 = 0L
        private const val snapshotVersion1 = 1L
        private const val streamVersion0 = 0L
        private const val streamVersion1 = 1L
        private const val streamVersion2 = 2L
        private const val streamVersion3 = 3L
        private const val streamReadPosition1 = 1L
        private const val streamReadPosition2 = 2L
        private const val streamReadPosition3 = 3L
        private const val readIndex1 = 1L
        private const val readIndex2 = 2L
        private const val readerId = "some_reader"
    }

    @Autowired
    private lateinit var databaseConnectionFactory: ru.quipy.db.factory.ConnectionFactory

    @Autowired
    @Qualifier("postgresClientEventStore")
    private lateinit var postgresClientEventStore : EventStore

    @Autowired
    @Qualifier("postgresTemplateEventStore")
    private lateinit var postgresTemplateEventStore: EventStore

    @Autowired
    private lateinit var executor: QueryExecutor

    @Autowired
    private lateinit var entityConverter: EntityConverter;

    @Value("\${defaultSchema:event_sourcing_store}")
    private lateinit var schema: String

    @BeforeEach
    fun truncateAll() {
        databaseConnectionFactory.getDatabaseConnection().prepareStatement(
            "truncate ${schema}.${EventRecordTable.name};" +
                "truncate ${schema}.${SnapshotTable.name};" +
                "truncate ${schema}.${EventStreamReadIndexTable.name};" +
                "truncate ${schema}.${EventStreamActiveReadersTable.name};"
        ).execute()
    }
    @Test
    fun testInsertSingleEventRecordRecordsAndCheckSelect() {
        insertEventRecordAndCheck(postgresClientEventStore)
        truncateAll()
        insertEventRecordAndCheck(postgresTemplateEventStore)
    }

    @Test
    fun testBatchInsertEventRecordsAndCheckSelectWithConditionsAndLimit() {
        // insertEventRecordsAndCheckSelect(postgresClientEventStore)
        // truncateAll()
        insertEventRecordsAndCheckSelect(postgresTemplateEventStore)
    }

    @Test
    fun testFindEntityById() {
        testFindEntityById(postgresClientEventStore)
        truncateAll()
        testFindEntityById(postgresTemplateEventStore)
    }

    @Test
    fun testStreamReaders() {
        testStreamReaders(postgresClientEventStore)
        truncateAll()
        testStreamReaders(postgresTemplateEventStore)
    }
    private fun insertEventRecordAndCheck(eventStore: EventStore) {
        eventStore.insertEventRecord(aggregateTableName, generateEventRecord(1, aggregateId1, aggregateVersion1, timestamp1))
        var eventRecord = eventStore.findEventRecordsWithAggregateVersionGraterThan(aggregateTableName, aggregateId1, 0)
        Assertions.assertEquals(1, eventRecord.size)
    }

    private fun insertEventRecordsAndCheckSelect(eventStore: EventStore) {
        eventStore.insertEventRecords(aggregateTableName, generateEventRecords(10, aggregateId1, aggregateVersion1, timestamp1))
        eventStore.insertEventRecords(aggregateTableName, generateEventRecords(5, aggregateId1, aggregateVersion2, timestamp1))
        eventStore.insertEventRecords(aggregateTableName, generateEventRecords(10, aggregateId2, aggregateVersion1, timestamp1))
        eventStore.insertEventRecords(aggregateTableName, generateEventRecords(11, aggregateId1, aggregateVersion1, timestamp2))
        eventStore.insertEventRecords(aggregateTableName, generateEventRecords(6, aggregateId3, aggregateVersion1, timestamp3))

        Assertions.assertEquals(5, eventStore.findEventRecordsWithAggregateVersionGraterThan(aggregateTableName, aggregateId1, aggregateVersion1).size)
        Assertions.assertEquals(26, eventStore.findEventRecordsWithAggregateVersionGraterThan(aggregateTableName, aggregateId1, aggregateVersion0).size)
        Assertions.assertEquals(0, eventStore.findEventRecordsWithAggregateVersionGraterThan(aggregateTableName, aggregateId1, aggregateVersion2).size)
        Assertions.assertEquals(0, eventStore.findEventRecordsWithAggregateVersionGraterThan(aggregateTableName, aggregateId1, aggregateVersion3).size)
        Assertions.assertEquals(10, eventStore.findEventRecordsWithAggregateVersionGraterThan(aggregateTableName, aggregateId2, aggregateVersion0).size)

        Assertions.assertEquals(17, eventStore.findBatchOfEventRecordAfter(aggregateTableName, timestamp1, 100).size)
        Assertions.assertEquals(42, eventStore.findBatchOfEventRecordAfter(aggregateTableName, timestamp0, 100).size)
        Assertions.assertEquals(35, eventStore.findBatchOfEventRecordAfter(aggregateTableName, timestamp0, 35).size)
        Assertions.assertEquals(0, eventStore.findBatchOfEventRecordAfter(aggregateTableName, timestamp3, 100).size)
    }

    private fun generateEventRecords(number: Int, aggregateId: String, aggregateVersion: Long, timestamp: Long) : List<EventRecord> {
        var ans = mutableListOf<EventRecord>()
        for (i in 1..number) {
            ans.add(generateEventRecord(i.toLong(), aggregateId, aggregateVersion, timestamp))
        }
        return ans
    }
    private fun generateEventRecord(id: Long, aggregateId: String, aggregateVersion: Long, timestamp: Long) : EventRecord {
        return EventRecord(
            id.toString(), aggregateId, aggregateVersion, "test_event", "{}", SagaContext(), timestamp
        )
    }

    private fun testFindEntityById(eventStore: EventStore) {
        var snapshot = Snapshot(snapshotId1, "", snapshotVersion1)
        var streamReadIndex = EventStreamReadIndex(streamName, 1, streamVersion1)
        var activeEventStreamReader = ActiveEventStreamReader(streamName, streamVersion1, readerId, 1, timestamp1)

        insertEntity(snapshot)
        insertEntity(streamReadIndex)
        insertEntity(activeEventStreamReader)

        var snapshotFromDb = eventStore.findSnapshotByAggregateId(snapshotsTableName, snapshotId1)
        var streamReadIndexFromDb = eventStore.findStreamReadIndex(streamName)
        var activeStreamReaderFromDb =  eventStore.getActiveStreamReader(streamName)
        Assertions.assertNotNull(snapshotFromDb)
        Assertions.assertNotNull(streamReadIndexFromDb)
        Assertions.assertNotNull(activeStreamReaderFromDb)

        Assertions.assertNull(eventStore.findSnapshotByAggregateId(snapshotsTableName, snapshotId2))
        Assertions.assertNull(eventStore.findStreamReadIndex(otherStreamName))
        Assertions.assertNull(eventStore.getActiveStreamReader(otherStreamName))
    }

    private fun <E: Any> insertEntity(entity: E) {
        val dto = when(entity::class) {
            EventRecord::class -> EventRecordDto(entity as EventRecord, aggregateTableName, entityConverter)
            Snapshot::class -> SnapshotDto(entity as Snapshot, snapshotsTableName, entityConverter)
            EventStreamReadIndex::class -> EventStreamReadIndexDto(entity as EventStreamReadIndex)
            ActiveEventStreamReader::class -> ActiveEventStreamReaderDto(entity as ActiveEventStreamReader)
            else -> throw UnknownEntityClassException(entity::class.java.name)
        }

        executor.execute(QueryBuilder.insert(schema, dto))
    }

    private fun testStreamReaders(eventStore: EventStore) {
        var activeEventStreamReader = ActiveEventStreamReader(streamName, streamVersion1, readerId, streamReadPosition1, timestamp1)

        // inserting
        Assertions.assertTrue(eventStore.tryUpdateActiveStreamReader(activeEventStreamReader))

        // updating with wrong version
        var newActiveEventStreamReader = ActiveEventStreamReader(streamName, streamVersion2, readerId, streamReadPosition2, timestamp2)
        Assertions.assertTrue(eventStore.tryReplaceActiveStreamReader(streamVersion2, newActiveEventStreamReader))

        // version remains the same
        var activeReaderFromDb = eventStore.getActiveStreamReader(streamName)
        Assertions.assertEquals(streamVersion1, activeReaderFromDb?.version)

        // updating with correct version
        Assertions.assertTrue(eventStore.tryReplaceActiveStreamReader(streamVersion1, newActiveEventStreamReader))
        Assertions.assertTrue(eventStore.tryReplaceActiveStreamReader(streamVersion1, newActiveEventStreamReader)) // проверка на идемпотентность

        var otherActiveStreamReader = ActiveEventStreamReader(streamName, streamVersion3, readerId, streamReadPosition3, timestamp2)
        Assertions.assertTrue(eventStore.tryUpdateActiveStreamReader(otherActiveStreamReader))

        activeReaderFromDb = eventStore.getActiveStreamReader(streamName)
        // stream updated
        Assertions.assertEquals(streamVersion3, activeReaderFromDb?.version)
        Assertions.assertEquals(streamVersion3, activeReaderFromDb?.readPosition)
        Assertions.assertEquals(timestamp2, activeReaderFromDb?.lastInteraction)

        var streamReadIndex = EventStreamReadIndex(streamName, readIndex1, streamVersion1)
        Assertions.assertTrue(eventStore.commitStreamReadIndex(streamReadIndex))
        var otherStreamReadIndex = EventStreamReadIndex(streamName, readIndex2, streamVersion2)
        Assertions.assertTrue(eventStore.commitStreamReadIndex(otherStreamReadIndex))

        var readIndexFromDb = eventStore.findStreamReadIndex(streamName)
        Assertions.assertEquals(readIndex2, readIndexFromDb?.readIndex)
    }
}