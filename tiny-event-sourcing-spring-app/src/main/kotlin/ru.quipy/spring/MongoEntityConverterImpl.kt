package ru.quipy.spring

import org.bson.Document
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoTemplate

class MongoEntityConverterImpl {

    @Autowired
    private lateinit var mongoTemplate: MongoTemplate

    fun <T : Any> convertObjectToBsonDocument(obj : T) : Document {
        val document = Document()
        mongoTemplate.converter.write(obj, document)
        return document
    }

    fun <T> convertBsonDocumentToObject(document: Document, clazz: Class<T>) : T {
        return mongoTemplate.converter.read(clazz, document)
    }

}