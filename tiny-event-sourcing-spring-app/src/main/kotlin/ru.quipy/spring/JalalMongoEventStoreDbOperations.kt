package ru.quipy.spring

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Filters.gt
import com.mongodb.client.model.Sorts
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.where
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*


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

    private fun objectToDocument(entity: Any) : Document {
        val mappedObject = objectMapper.convertValue(entity, object:
            TypeReference<Map<String, Any>>() {});
        val document = Document(mappedObject)
        if(!document.containsKey("_id")){
            val id = document["id"];
            if(id != null) {
                document["_id"] = id
                document.remove("id")
            }
        }
        return document;
    }


    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        val document = this.objectToDocument(eventRecord)
        val database = mongoClient.getDatabase("tiny-es");
        val collection = database.getCollection(aggregateTableName)
        println(document)
        collection.insertOne(document);

//        try {
//            val database = mongoClient.getDatabase("tiny-es");
//            val command = BsonDocument("ping", BsonInt64(1));
//            val collection = database.getCollection(aggregateTableName);
//            var gson = Gson()
//            val result = collection.insertOne(Document.parse(gson.toJson(eventRecord)));
//            val commandResult = database.runCommand(command);
//
//            println("Connected successfully to server.");
//            println(commandResult)
//            val client = KMongo.createClient("mongodb://localhost:27017") //get com.mongodb.MongoClient new instance
//
//        } catch (e: DuplicateKeyException) {
//            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventRecord", e)
//        }
//        try {
//            mongoTemplate.insert(eventRecord, aggregateTableName)
//        } catch (e: DuplicateKeyException) {
//            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventRecord", e)
//        }
    }

    override fun tableExists(aggregateTableName: String) = mongoTemplate.collectionExists(aggregateTableName)

    override fun updateSnapshotWithLatestVersion(tableName: String, snapshot: Snapshot) {
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
        val list : List<EventRecord> = document
            .map{map ->
                println(map)
                map.put("id", map.get("_id"))
                map.remove("_id")
                map
            }
            .map{map -> objectMapper.convertValue(map, object:TypeReference<EventRecord>() {})}
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
        val list : List<EventRecord> = document
            .map{map ->
                println(map)
                map.put("id", map.get("_id"))
                map.remove("_id")
                map
            }
            .map{map -> objectMapper.convertValue(map, object:TypeReference<EventRecord>() {})}
        return list
    }

    override fun findSnapshotByAggregateId(snapshotsTableName: String, aggregateId: Any): Snapshot? {
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