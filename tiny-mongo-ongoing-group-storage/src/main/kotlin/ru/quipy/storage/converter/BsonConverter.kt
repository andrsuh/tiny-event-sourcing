package ru.quipy.storage.converter

interface BsonConverter<T, V> {
    fun convertToBsonType(value: Any): T?
    fun convertFromBsonType(value: Any): V?
}