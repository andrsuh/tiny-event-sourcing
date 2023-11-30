package jp.veka.converter

import java.sql.ResultSet
import kotlin.reflect.KClass

interface ResultSetToEntityMapper {
    fun <T : Any> convert(resultSet: ResultSet?, clazz: KClass<T>) : T?
    fun <T : Any> convertMany(resultSet: ResultSet?, clazz: KClass<T>) : List<T>
}