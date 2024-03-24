package ru.quipy.tables

import kotlin.reflect.KClass

class Column<T: Any> {
    val index: Int
    val name: String
    val type: KClass<T>

    constructor(index: Int, name: String, type: KClass<T>) {
        this.index = index
        this.name = name
        this.type = type
    }
}