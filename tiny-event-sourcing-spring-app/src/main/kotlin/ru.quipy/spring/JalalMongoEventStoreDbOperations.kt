package ru.quipy.spring

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.mongodb.client.MongoClient
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Filters.gt
import com.mongodb.client.model.Sorts
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findById
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*


//TODO Change class name
class JalalMongoDbEventStoreDbOperations : EventStoreDbOperations {
    companion object {
        val logger = LoggerFactory.getLogger(MongoDbEventStoreDbOperations::class.java)
    }

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    @Autowired
    lateinit var mongoClient: MongoClient

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var entityConverter: MongoEntityConverter

    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        val database = mongoClient.getDatabase("tiny-es")
        val collection = database.getCollection(aggregateTableName)
        val document = entityConverter.convertToDocument(eventRecord)
        collection.insertOne(document)
    }

    override fun tableExists(aggregateTableName: String) = mongoTemplate.collectionExists(aggregateTableName)

    override fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot) {
        val database = mongoClient.getDatabase("tiny-es")
        val collection = database.getCollection(tableName)
        val ow: ObjectWriter = ObjectMapper().writer().withDefaultPrettyPrinter()
        val json: String = ow.writeValueAsString(snapshot)
        System.out.println(entityConverter.convertToDocument(snapshot))
        System.out.println(entityConverter.convertToDocument(snapshot.snapshot))
        collection.find(eq("_id",snapshot.id))
        collection.insertOne(entityConverter.convertToDocument(snapshot))
        //collection.findOneAndUpdate(eq("_id",snapshot.id), entityConverter.convertToDocument(snapshot))
        mongoTemplate.updateWithLatestVersion(tableName, snapshot)
    }

    override fun findBatchOfEventRecordAfter(
        aggregateTableName: String,
        eventSequenceNum: Long,
        batchSize: Int
    ): List<EventRecord> {
        val database = mongoClient.getDatabase("tiny-es");
        val collection = database.getCollection(aggregateTableName)
        val document = collection.find(gt("createdAt", eventSequenceNum))
            .sort(Sorts.ascending("createdAt"))
            .limit(batchSize).toList()

        val list : List<EventRecord> = document.map{
            entityConverter.convertToObject(it, EventRecord::class.java)
        }
        return list
    }


    override fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: Any,
        aggregateVersion: Long
    ): List<EventRecord> {
        val database = mongoClient.getDatabase("tiny-es");
        val collection = database.getCollection(aggregateTableName)
        val document = collection.find(eq("aggregateId",aggregateId))
            .filter(gt("aggregateVersion",aggregateVersion)).toList()
        val list : List<EventRecord> = document.map{
            entityConverter.convertToObject(it, EventRecord::class.java)
        }
        return list
    }

    override fun findSnapshotByAggregateId(snapshotsTableName: String, aggregateId: Any): Snapshot? {
        val database = mongoClient.getDatabase("tiny-es");
        val collection = database.getCollection(snapshotsTableName)
        val document = collection.find(eq("aggregateId",aggregateId))
        return mongoTemplate.findById(aggregateId, snapshotsTableName)
    }

    override fun findStreamReadIndex(streamName: String): EventStreamReadIndex? {
        return mongoTemplate.findById(streamName, "event-stream-read-index") // todo sukhoa make configurable?
    }

    override fun getActiveStreamReader(streamName: String): ActiveEventStreamReader? {
        return mongoTemplate.findById(streamName, "event-stream-active-readers") // todo sukhoa make configurable?
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex) {
        mongoTemplate.updateWithLatestVersion("event-stream-read-index", readIndex) // todo sukhoa make configurable?
    }

}

//inline fun <reified E> MongoTemplate.replaceOlderEntityOrInsert(
//    tableName: String,
//    replacement: E
//): E? where E : Versioned, E : Unique<*> {
//    return update(E::class.java)
//        .inCollection(tableName)
//        .matching(Query.query(Criteria.where("_id").`is`(replacement.id).and("version").lt(replacement.version)))
//        .replaceWith(replacement)
//        .withOptions(FindAndReplaceOptions.options().upsert().returnNew())
//        .findAndReplace()
//        .orElse(null)
//}
//
//inline fun <reified E> MongoTemplate.updateWithLatestVersion(
//    tableName: String,
//    entity: E
//): E? where E : Versioned, E : Unique<*> = try {
//    replaceOlderEntityOrInsert(tableName, entity)
//} catch (e: DuplicateKeyException) {
//    logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
//    null
//}
//
//inline fun <reified E, ID> MongoTemplate.updateWithOptimisticLock(
//    tableName: String,
//    id: ID,
//    updateFunction: (E?) -> E
//): E where E : Versioned, E : Unique<ID>, ID : Any {
//    while (true) { // todo sukhoa while true ufff
//        val existing = findById(id, E::class.java, tableName)
//        val currentVersion = existing?.version ?: 1
//        val updated = updateFunction(existing).also {
//            it.version = currentVersion + 1
//        }
//
//        if (existing != null) {
//            val updateRes = update(E::class.java)
//                .inCollection(tableName)
//                .matching(Query.query(Criteria.where("_id").`is`(updated.id).and("version").`is`(currentVersion)))
//                .replaceWith(updated)
//                .findAndReplace()
//            if (updateRes.isPresent) return updated
//        } else {
//            try {
//                insert(E::class.java)
//                    .inCollection(tableName)
//                    .one(updated)
//            } catch (e: DuplicateKeyException) {
//                logger.info("Entity concurrent update led to clashing. Entity: $updated, table name: $tableName", e)
//                continue
//            }
//            return updated
//        }
//    }
//}