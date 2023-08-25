package ru.quipy.storage

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.OngoingGroupStorage
import ru.quipy.domain.EventAggregation
import ru.quipy.storage.converter.MongoEntityConverter
import ru.quipy.storage.exception.MongoClientExceptionTranslator
import ru.quipy.storage.exception.MongoDuplicateKeyException
import ru.quipy.storage.factory.MongoClientFactory

class MongoClientOngoingGroupStorage(
    private val entityConverter: MongoEntityConverter,
    private val databaseFactory: MongoClientFactory
) : OngoingGroupStorage {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MongoClientOngoingGroupStorage::class.java)
    }

    private val exceptionTranslator = MongoClientExceptionTranslator()

    override fun insertEventAggregation(tableName: String, eventAggregation: EventAggregation) {
        val document = entityConverter.convertObjectToBsonDocument(eventAggregation)
        try {
            exceptionTranslator.withTranslation {
                databaseFactory.getDatabase().getCollection(tableName).insertOne(document)
            }
        } catch (e: MongoDuplicateKeyException) {
            throw DuplicateEventIdException(
                "There is record with such an id. Record cannot be saved $eventAggregation", e
            )
        }
    }

    override fun findBatchOfEventAggregations(tableName: String): List<EventAggregation> {
        return databaseFactory.getDatabase()
            .getCollection(tableName)
            .find()
//            .find(gt("createdAt", 0))
//            .sort(Sorts.ascending("createdAt"))
////            .limit(batchSize)
            .toList()
            .map { entityConverter.convertBsonDocumentToObject(it, EventAggregation::class) }

    }

    override fun dropTable(tableName: String) {
        databaseFactory.getDatabase().getCollection(tableName).drop()
    }
}
