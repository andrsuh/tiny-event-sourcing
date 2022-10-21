package ru.quipy.spring

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.bson.Document
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.domain.EventRecord
import kotlin.reflect.KClass

class JacksonMongoEntityConverter : MongoEntityConverter{
    //TODO make bean
    private fun initMapper() : ObjectMapper {
        val mapper = ObjectMapper()
        val ptv: PolymorphicTypeValidator = BasicPolymorphicTypeValidator
            .builder()
            .allowIfBaseType("java")
            .build()
        val typeResolver: TypeResolverBuilder<*> = ObjectMapper.DefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT, ptv)
        typeResolver.init(JsonTypeInfo.Id.CLASS, null)
        typeResolver.inclusion(JsonTypeInfo.As.PROPERTY)
        typeResolver.typeProperty("_class")
        mapper.setDefaultTyping(typeResolver)
        mapper.registerModule(KotlinModule())
        return mapper;
    }

    private val objectMapper: ObjectMapper = initMapper()

    override fun <T : Any> convertObjectToBsonDocument(obj : T) : Document {
        val document = Document.parse(objectMapper.writeValueAsString(obj))
        if(document.containsKey("id")){
            document["_id"] = document.remove("id");
        }
        return document;
    }

    override fun <T : Any> convertBsonDocumentToObject(document: Document, clazz: KClass<T>): T {
        if(document.containsKey("_id")){
            document["id"] = document.remove("_id");
        }
        return objectMapper.readValue(document.toJson(), clazz.java)
    }

}