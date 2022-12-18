package ru.quipy.eventstore.converter

import org.bson.Document
import kotlin.reflect.KClass

interface MongoEntityConverter {
    fun <T : Any> convertObjectToBsonDocument(obj: T): Document
    fun <T : Any> convertBsonDocumentToObject(document: Document, clazz: KClass<T>): T
}