package ru.quipy.converter

import kotlin.reflect.KClass

interface EntityConverter {
    fun <T : Any> serialize(obj: T): String
    fun <T : Any> toObject(converted: String, clazz: KClass<T>): T
    fun <T : Any> toNullableObject(converted: String, clazz: KClass<T>): T?
}