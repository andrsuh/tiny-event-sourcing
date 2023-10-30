package jp.veka.converter

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KClass

class JsonEntityConverter : EntityConverter {
    private val objectMapper = jacksonObjectMapper()
    override fun <T : Any> serialize(obj: T): String {
        return objectMapper.writeValueAsString(obj)
    }

    override fun <T : Any> toObject(converted: String, clazz: KClass<T>): T {
        return objectMapper.readValue(converted, clazz.java)
    }
}