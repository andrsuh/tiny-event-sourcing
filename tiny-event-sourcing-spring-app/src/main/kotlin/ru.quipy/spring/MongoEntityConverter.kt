package ru.quipy.spring

import com.fasterxml.jackson.core.type.TypeReference
import org.bson.Document

interface MongoEntityConverter {
    fun <T : Any> convertObjectToBsonDocument(obj : T) : Document
    fun <T> convertBsonDocumentToObject(document: Document, typeRef: TypeReference<T>) : T
}