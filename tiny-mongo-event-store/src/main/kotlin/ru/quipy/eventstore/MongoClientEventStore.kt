package ru.quipy.eventstore

import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.EventStore
import ru.quipy.domain.*
import ru.quipy.eventstore.converter.MongoEntityConverter
import ru.quipy.eventstore.exception.MongoClientExceptionTranslator
import ru.quipy.eventstore.exception.MongoDuplicateKeyException
import ru.quipy.eventstore.factory.MongoClientFactory

class MongoClientEventStore(
    private val entityConverter: MongoEntityConverter,
    private val databaseFactory: MongoClientFactory
) : EventStore {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MongoClientEventStore::class.java)
    }

    private val exceptionTranslator = MongoClientExceptionTranslator()

    override fun insertEventRecord(aggregateTableName: String, eventRecord: EventRecord) {
        val document = entityConverter.convertObjectToBsonDocument(eventRecord)
        try {
            exceptionTranslator.withTranslation {
                databaseFactory.getDatabase().getCollection(aggregateTableName).insertOne(document)
            }
        } catch (e: MongoDuplicateKeyException) {
            throw DuplicateEventIdException(
                "There is record with such an id. Record cannot be saved $eventRecord", e
            )
        }
    }

    override fun insertEventRecords(aggregateTableName: String, eventRecords: List<EventRecord>) {
        val session = databaseFactory.getClient().startSession()
        session.use { clientSession ->
            try {
                exceptionTranslator.withTranslation {
                    clientSession.startTransaction()
                    databaseFactory.getDatabase().getCollection(aggregateTableName).insertMany(
                        eventRecords.map { entityConverter.convertObjectToBsonDocument(it) },
                        InsertManyOptions().ordered(true)
                    )
                    clientSession.commitTransaction()
                }
            } catch (e: MongoDuplicateKeyException) {
                clientSession.abortTransaction()
                throw DuplicateEventIdException(
                    "There is record with such an id. Records cannot be saved $eventRecords", e
                )
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

    override fun tryUpdateActiveStreamReader(updatedActiveReader: ActiveEventStreamReader): Boolean {
        return insetOrUpdateEntityByIdAndVersion("event-stream-active-readers", updatedActiveReader, expectedVersion = updatedActiveReader.version - 1) != null
    }

    override fun tryReplaceActiveStreamReader(expectedVersion: Long, newActiveReader: ActiveEventStreamReader): Boolean {
        return tryReplaceWithOptimisticLock("event-stream-active-readers", expectedVersion, newActiveReader)
    }

    override fun commitStreamReadIndex(readIndex: EventStreamReadIndex): Boolean {
        return updateWithLatestVersion("event-stream-read-index", readIndex) != null // todo sukhoa make configurable?
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
        exceptionTranslator.withTranslation {
            replaceOlderEntityOrInsert(tableName, entity)
        }
    } catch (e: MongoDuplicateKeyException) {
        logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
        null
    }

    private inline fun <reified T> insetOrUpdateEntityByIdAndVersion(
        tableName: String, replacement: T, expectedVersion: Long? = null
    ): T? where T : Versioned, T : Unique<*> {
        val document = entityConverter.convertObjectToBsonDocument(replacement)
        document.remove("_id")

        val result = databaseFactory.getDatabase()
            .getCollection(tableName)
            .findOneAndReplace(
                and(eq("_id", replacement.id), eq("version", expectedVersion ?: replacement.version)),
                document,
                FindOneAndReplaceOptions()
                    .upsert(true)
                    .returnDocument(ReturnDocument.AFTER)
            ) ?: return null

        return entityConverter.convertBsonDocumentToObject(result, T::class)
    }

    private inline fun <reified E> tryReplaceWithOptimisticLock(
        tableName: String,
        expectedVersion: Long,
        entity: E
    ): Boolean where E : Versioned, E : Unique<*> {
        while (true) {
            val existingDocument = findOne(tableName, entity.id!!)

            if (existingDocument != null) {
                val updateResult = insetOrUpdateEntityByIdAndVersion(tableName, entity, expectedVersion)
                return updateResult != null
            } else {
                try {
                    val document = entityConverter.convertObjectToBsonDocument(entity)

                    exceptionTranslator.withTranslation {
                        databaseFactory.getDatabase().getCollection(tableName).insertOne(document)
                    }

                    return true
                } catch (e: MongoDuplicateKeyException) {
                    logger.info("Entity concurrent update led to clashing. Entity: $entity, table name: $tableName", e)
                    continue
                }
            }
        }
    }
}
