package jp.veka.mappers

import org.springframework.jdbc.core.RowMapper
import kotlin.reflect.KClass

interface MapperFactory {
    fun <T : Any> getMapper(clazz: KClass<T>): RowMapper<T>
}