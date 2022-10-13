package ru.quipy.spring

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.DuplicateKeyException
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
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.demo.logic.ProjectAggregateState
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

    @Autowired
    lateinit var objectMapper: ObjectMapper;


    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        val document = entityConverter.convertObjectToBsonDocument(eventRecord)
        try {
            getDatabase().getCollection(aggregateTableName).insertOne(document)
        }catch(e: DuplicateKeyException) {
            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventRecord", e)
        }
    }

    override fun tableExists(aggregateTableName: String) : Boolean {
        return getDatabase()
            .listCollectionNames()
            .into(ArrayList<String>())
            .contains(aggregateTableName)
    }

    override fun <T, E> updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot<T, E>) {
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
            .map{ entityConverter.convertBsonDocumentToObject(it, object : TypeReference<EventRecord>() {}) }
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
            )).toList().map{ entityConverter.convertBsonDocumentToObject(it, object : TypeReference<EventRecord>() {}) }
    }

    override fun <T, E> findSnapshotByAggregateId(snapshotsTableName: String, aggregateId: Any): Snapshot<T, E>? {
        val document = findOne(snapshotsTableName, aggregateId) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, object : TypeReference<Snapshot<T, E>>() {})
    }

    override fun findStreamReadIndex(streamName: String): EventStreamReadIndex? {
        val document = findOne("event-stream-read-index", streamName) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, object : TypeReference<EventStreamReadIndex>() {})
    }

    override fun getActiveStreamReader(streamName: String): ActiveEventStreamReader? {
        val document = findOne("event-stream-active-readers", streamName) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, object : TypeReference<ActiveEventStreamReader>() {})
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
        val result = getDatabase()
            .getCollection(tableName)
            .findOneAndReplace(
                and(eq("_id", replacement.id), lt("version", replacement.version)),
                entityConverter.convertObjectToBsonDocument(replacement),
                FindOneAndReplaceOptions()
                    .upsert(true)
                    .returnDocument(ReturnDocument.AFTER)
            ) ?: return null

        return entityConverter.convertBsonDocumentToObject(result, object : TypeReference<T>() {})
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
