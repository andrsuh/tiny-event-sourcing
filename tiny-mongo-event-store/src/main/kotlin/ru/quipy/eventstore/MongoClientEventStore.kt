package ru.quipy.eventstore

import com.mongodb.ErrorCategory
import com.mongodb.MongoCommandException
import com.mongodb.MongoWriteException
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStore
import ru.quipy.domain.*
import ru.quipy.eventstore.converter.MongoEntityConverter
import ru.quipy.eventstore.factory.MongoClientFactory

class MongoClientEventStore(
    private val entityConverter: MongoEntityConverter,
    private val databaseFactory: MongoClientFactory
) : EventStore {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MongoClientEventStore::class.java)
    }

    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        val document = entityConverter.convertObjectToBsonDocument(eventRecord)
        try {
            databaseFactory.getDatabase().getCollection(aggregateTableName).insertOne(document)
        } catch (e: MongoWriteException) {
            if (ErrorCategory.fromErrorCode(e.code) == ErrorCategory.DUPLICATE_KEY) {
                throw DuplicateEventIdException(
                    "There is record with such an id. Record cannot be saved $eventRecord",
                    e
                )
            } else throw e
        }
    }

    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        val session = databaseFactory.getClient().startSession();
        session.use { clientSession ->
            try {
                clientSession.startTransaction()
                databaseFactory.getDatabase().getCollection(aggregateTableName).insertMany(
                    eventRecords.map { entityConverter.convertObjectToBsonDocument(it) }
                )
                clientSession.commitTransaction()
            } catch (e: MongoCommandException) {
                clientSession.abortTransaction()
                if (ErrorCategory.fromErrorCode(e.errorCode) == ErrorCategory.DUPLICATE_KEY) {
                    throw DuplicateEventIdException(
                        "There is record with such an id. Records cannot be saved $eventRecords",
                        e
                    )
                } else throw e
            } catch (e: Exception) {
                clientSession.abortTransaction()
            }
        }
    }

    override fun tableExists(aggregateTableName: String): Boolean {
        return databaseFactory.getDatabase()
            .listCollectionNames()
            .into(ArrayList<String>())
            .contains(aggregateTableName)
    }

    override fun updateSnapshotWithLatestVersion(
        tableName: String,
        snapshot: Snapshot
    ) {
        updateWithLatestVersion(tableName, snapshot)
    }

    override fun findBatchOfEventRecordAfter(
        aggregateTableName: String,
        eventSequenceNum: Long,
        batchSize: Int
    ): List<EventRecord> {

        return databaseFactory.getDatabase()
            .getCollection(aggregateTableName)
            .find(gt("createdAt", eventSequenceNum))
            .sort(Sorts.ascending("createdAt"))
            .limit(batchSize)
            .toList()
            .map { entityConverter.convertBsonDocumentToObject(it, EventRecord::class) }
    }


    override fun findEventRecordsWithAggregateVersionGraterThan(
        aggregateTableName: String,
        aggregateId: Any,
        aggregateVersion: Long
    ): List<EventRecord> {

        return databaseFactory.getDatabase()
            .getCollection(aggregateTableName)
            .find(
                and(
                    eq("aggregateId", aggregateId),
                    gt("aggregateVersion", aggregateVersion)
                )
            ).toList().map { entityConverter.convertBsonDocumentToObject(it, EventRecord::class) }
    }

    override fun findSnapshotByAggregateId(
        snapshotsTableName: String,
        aggregateId: Any
    ): Snapshot? {
        val document = findOne(snapshotsTableName, aggregateId) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, Snapshot::class)
    }

    override fun findStreamReadIndex(streamName: String): EventStreamReadIndex? {
        val document = findOne("event-stream-read-index", streamName) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, EventStreamReadIndex::class)
    }

    override fun getActiveStreamReader(streamName: String): ActiveEventStreamReader? {
        val document = findOne("event-stream-active-readers", streamName) ?: return null
        return entityConverter.convertBsonDocumentToObject(document, ActiveEventStreamReader::class)
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex) {
        updateWithLatestVersion("event-stream-read-index", readIndex) // todo sukhoa make configurable?
    }

    private fun findOne(collectionName: String, id: Any): Document? {
        return databaseFactory.getDatabase()
            .getCollection(collectionName)
            .find(eq("_id", id))
            .first()
    }

    private inline fun <reified T> replaceOlderEntityOrInsert(
        tableName: String, replacement: T
    ): T? where T : Versioned, T : Unique<*> {
        val document = entityConverter.convertObjectToBsonDocument(replacement);
        document.remove("_id")
        val result = databaseFactory.getDatabase()
            .getCollection(tableName)
            .findOneAndReplace(
                and(eq("_id", replacement.id), lt("version", replacement.version)),
                document,
                FindOneAndReplaceOptions()
                    .upsert(true)
                    .returnDocument(ReturnDocument.AFTER)
            ) ?: return null

        return entityConverter.convertBsonDocumentToObject(result, T::class)
    }

    private inline fun <reified E> updateWithLatestVersion(
        tableName: String,
        entity: E
    ): E? where E : Versioned, E : Unique<*> = try {
        replaceOlderEntityOrInsert(tableName, entity)
    } catch (e: MongoCommandException) {
        if (ErrorCategory.fromErrorCode(e.errorCode) == ErrorCategory.DUPLICATE_KEY) {
            logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
            null
        } else throw e
    }
}
