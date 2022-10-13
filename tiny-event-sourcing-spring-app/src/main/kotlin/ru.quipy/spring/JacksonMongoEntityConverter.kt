package ru.quipy.spring

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.bson.Document
import org.springframework.beans.factory.annotation.Autowired

class JacksonMongoEntityConverter : MongoEntityConverter {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    override fun <T : Any> convertObjectToBsonDocument(obj : T) : Document {
        val document = Document.parse(objectMapper.writeValueAsString(obj))
        if(document.containsKey("id")) {
            document["_id"] = document.remove("id");
        }
        return document;
    }
    override fun <T> convertBsonDocumentToObject(document: Document, typeRef: TypeReference<T>) : T {
        println(typeRef.type)
        if(document.containsKey("_id")) {
            document["id"] = document.remove("_id");
        }
        return objectMapper.readValue(document.toJson(), typeRef)
    }
}