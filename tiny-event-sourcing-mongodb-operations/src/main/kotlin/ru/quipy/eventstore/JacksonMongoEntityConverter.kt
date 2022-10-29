package ru.quipy.eventstore

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.bson.Document
import kotlin.reflect.KClass

private const val TYPE_PROPERTY_KEY = "_class"
private const val BASE_TYPE_PREFIX = "java"
private const val EXPECTED_ID_KEY = "id"
private const val TARGET_ID_KEY = "_id"

class JacksonMongoEntityConverter : MongoEntityConverter {

    private val objectMapper: ObjectMapper = initMapper()

    private fun initMapper(): ObjectMapper {
        val mapper = ObjectMapper()
        val polymorphicTypeValidator: PolymorphicTypeValidator = BasicPolymorphicTypeValidator
            .builder()
            .allowIfBaseType(BASE_TYPE_PREFIX)
            .build()
        val typeResolver: TypeResolverBuilder<*> = ObjectMapper.DefaultTypeResolverBuilder(
            ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT,
            polymorphicTypeValidator
        )
        typeResolver.init(JsonTypeInfo.Id.CLASS, null)
        typeResolver.inclusion(JsonTypeInfo.As.PROPERTY)
        typeResolver.typeProperty(TYPE_PROPERTY_KEY)

        mapper.setDefaultTyping(typeResolver)
        mapper.registerModule(KotlinModule())
        return mapper
    }

    override fun <T : Any> convertObjectToBsonDocument(obj: T): Document {
        val document = Document.parse(objectMapper.writeValueAsString(obj))
        if (document.containsKey(EXPECTED_ID_KEY)) {
            document[TARGET_ID_KEY] = document.remove(EXPECTED_ID_KEY)
        }
        return document
    }

    override fun <T : Any> convertBsonDocumentToObject(document: Document, clazz: KClass<T>): T {
        if (document.containsKey(TARGET_ID_KEY)) {
            document[EXPECTED_ID_KEY] = document.remove(TARGET_ID_KEY)
        }
        return objectMapper.readValue(document.toJson(), clazz.java)
    }

}