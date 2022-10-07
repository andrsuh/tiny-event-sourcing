package ru.quipy.spring

import com.fasterxml.jackson.databind.ObjectMapper
import org.bson.Document
import org.springframework.beans.factory.annotation.Autowired

class MongoEntityConverter {

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private var allowedKeys = listOf("id", "_id")

    private var idKey = "_id";


    private fun documentIdKeyChanger(document: Document) : Document {
        allowedKeys.forEach {
            if(document.containsKey(it)){
                document["_id"] = document.remove(it)
            }
        }
        return document
    }

    private fun <T> documentIdKeyChanger(document: Document, clazz: Class<T>) : Document {
        clazz.declaredFields
            .filter { allowedKeys.contains(it.name) }
            .forEach {
                document[it.name] = document.remove("_id");
            }
        return document
    }

    fun <T> convertToDocument(obj : T) : Document {
        return documentIdKeyChanger(
            Document.parse(objectMapper.writeValueAsString(obj))
        )
    }

    fun <T> convertToObject(document: Document, clazz: Class<T>) : T {
        return objectMapper.readValue(
            documentIdKeyChanger(document, clazz).toJson(), clazz
        )
    }
}