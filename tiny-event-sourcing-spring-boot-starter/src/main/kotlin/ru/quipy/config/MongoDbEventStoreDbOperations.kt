package ru.quipy.config

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.FindAndReplaceOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.*
import ru.quipy.config.MongoDbEventStoreDbOperations.Companion.logger

class MongoDbEventStoreDbOperations : EventStoreDbOperations {
    companion object {
        val logger = LoggerFactory.getLogger(MongoDbEventStoreDbOperations::class.java)
    }

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        try {
            mongoTemplate.insert(eventRecord, aggregateTableName)
        } catch (e: DuplicateKeyException) {
            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventRecord", e)
        }
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
        val query = Query()
            .addCriteria(Criteria.where("createdAt").gt(eventSequenceNum))
            .with(Sort.by("createdAt").ascending())
            .limit(batchSize)

        return mongoTemplate.find(query, EventRecord::class.java, aggregateTableName)
    }


    override fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: Any,
        aggregateVersion: Long
    ): List<EventRecord> {
        val criteria = Criteria
            .where("aggregateId").`is`(aggregateId)
            .and("aggregateVersion").gt(aggregateVersion)

        return mongoTemplate.find(Query().addCriteria(criteria), aggregateTableName)
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

inline fun <reified E> MongoTemplate.replaceOlderEntityOrInsert(
    tableName: String,
    replacement: E
): E? where E : Versioned, E : Unique<*> {
    return update(E::class.java)
        .inCollection(tableName)
        .matching(Query.query(Criteria.where("_id").`is`(replacement.id).and("version").lt(replacement.version)))
        .replaceWith(replacement)
        .withOptions(FindAndReplaceOptions.options().upsert().returnNew())
        .findAndReplace()
        .orElse(null)
}

inline fun <reified E> MongoTemplate.updateWithLatestVersion(
    tableName: String,
    entity: E
): E? where E : Versioned, E : Unique<*> = try {
    replaceOlderEntityOrInsert(tableName, entity)
} catch (e: DuplicateKeyException) {
    logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
    null
}

inline fun <reified E, ID> MongoTemplate.updateWithOptimisticLock(
    tableName: String,
    id: ID,
    updateFunction: (E?) -> E
): E where E : Versioned, E : Unique<ID>, ID : Any {
    while (true) { // todo sukhoa while true ufff
        val existing = findById(id, E::class.java, tableName)
        val currentVersion = existing?.version ?: 1
        val updated = updateFunction(existing).also {
            it.version = currentVersion + 1
        }

        if (existing != null) {
            val updateRes = update(E::class.java)
                .inCollection(tableName)
                .matching(Query.query(Criteria.where("_id").`is`(updated.id).and("version").`is`(currentVersion)))
                .replaceWith(updated)
                .findAndReplace()
            if (updateRes.isPresent) return updated
        } else {
            try {
                insert(E::class.java)
                    .inCollection(tableName)
                    .one(updated)
            } catch (e: DuplicateKeyException) {
                logger.info("Entity concurrent update led to clashing. Entity: $updated, table name: $tableName", e)
                continue
            }
            return updated
        }
    }
}