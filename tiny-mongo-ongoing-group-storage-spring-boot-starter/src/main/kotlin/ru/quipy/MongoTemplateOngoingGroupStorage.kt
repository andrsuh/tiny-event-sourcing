package ru.quipy

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Query
import ru.quipy.core.exceptions.DuplicateEventIdException
import ru.quipy.database.OngoingGroupStorage
import ru.quipy.domain.EventAggregation


open class MongoTemplateOngoingGroupStorage : OngoingGroupStorage {
    companion object {
        val logger = LoggerFactory.getLogger(MongoTemplateOngoingGroupStorage::class.java)
    }

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    override fun insertEventAggregation(tableName: String, eventAggregation: EventAggregation) {
        try {
            mongoTemplate.insert(eventAggregation, tableName)
        } catch (e: DuplicateKeyException) {
            throw DuplicateEventIdException("There is record with such an id. Record cannot be saved $eventAggregation", e)
        }
    }

    override fun findBatchOfEventAggregations(tableName: String): List<EventAggregation> {
        val query = Query()

        return mongoTemplate.find(query, EventAggregation::class.java, tableName)

    }

    override fun dropTable(tableName: String) {
        return mongoTemplate.dropCollection(tableName)
    }
}