package ru.quipy.spring

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DuplicateKeyException
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*


//TODO Sevabeat change class name
class JalalMongoDbEventStoreDbOperations : EventStoreDbOperations {
    companion object {
        val logger = LoggerFactory.getLogger(MongoDbEventStoreDbOperations::class.java)
    }

    @Autowired
    lateinit var mongoClient: MongoClient

    @Autowired
    lateinit var entityConverter: MongoEntityConverter

    @Value("\${spring.data.mongodb.database}")
    lateinit var databaseName: String


    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        val document = entityConverter.convertObjectToBsonDocument(eventRecord)
        getDatabase().getCollection(aggregateTableName).insertOne(document)
    }

    override fun tableExists(aggregateTableName: String) : Boolean {
        return getDatabase()
            .listCollectionNames()
            .into(ArrayList<String>())
            .contains(aggregateTableName)
    }

    override fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot) {
        updateWithLatestVersion(tableName, snapshot);
    }

    override fun findBatchOfEventRecordAfter(
        aggregateTableName: String,
        eventSequenceNum: Long,
        batchSize: Int
    ): List<EventRecord> {

        return getDatabase()
            .getCollection(aggregateTableName)
            .find(gt("createdAt", eventSequenceNum))
            .sort(Sorts.ascending("createdAt"))
            .limit(batchSize)
            .toList()
            .map{ entityConverter.convertBsonDocumentToObject(it, EventRecord::class.java) }
    }


    override fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: Any,
        aggregateVersion: Long
    ): List<EventRecord> {

        return getDatabase()
            .getCollection(aggregateTableName)
            .find(and(
                eq("aggregateId", aggregateId),
                gt("aggregateVersion", aggregateVersion)
            )).toList().map{ entityConverter.convertBsonDocumentToObject(it, EventRecord::class.java) }
    }

    override fun findSnapshotByAggregateId(snapshotsTableName: String, aggregateId: Any): Snapshot? {
        val document = findOne(snapshotsTableName, aggregateId) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, Snapshot::class.java)
    }

    override fun findStreamReadIndex(streamName: String): EventStreamReadIndex? {
        val document = findOne("event-stream-read-index", streamName) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, EventStreamReadIndex::class.java)
    }

    override fun getActiveStreamReader(streamName: String): ActiveEventStreamReader? {
        val document = findOne("event-stream-active-readers", streamName) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, ActiveEventStreamReader::class.java)
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex) {
        updateWithLatestVersion("event-stream-read-index", readIndex) // todo sukhoa make configurable?
    }

    private fun getDatabase() : MongoDatabase {
        return mongoClient.getDatabase(databaseName)
    }

    private fun findOne(collectionName: String, id: Any) : Document? {
       return getDatabase()
            .getCollection(collectionName)
            .find(eq("_id", id))
            .first()
    }

    private inline fun <reified T> replaceOlderEntityOrInsert(
        tableName: String, replacement: T
    ): T? where T : Versioned, T : Unique<*> {
        val database = mongoClient.getDatabase("tiny-es")
        val collection = database.getCollection(tableName)
        val result = collection.findOneAndReplace(
            and(eq("_id", replacement.id), lt("version", replacement.version)),
            entityConverter.convertObjectToBsonDocument(replacement),
            FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
        ) ?: return null

        return entityConverter.convertBsonDocumentToObject(result, T::class.java)
    }

    private inline fun <reified E> updateWithLatestVersion(
        tableName: String,
        entity: E
    ): E? where E : Versioned, E : Unique<*> = try {
        replaceOlderEntityOrInsert(tableName, entity)
    } catch (e: DuplicateKeyException) {
        logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
        null
    }
}