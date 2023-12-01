package jp.veka.query

import jp.veka.query.exception.InvalidQueryStateException
import java.sql.PreparedStatement

abstract class BasicQuery<T: Query>(protected val schema: String, protected val relation: String) : Query {
    protected val columns: MutableList<String> = mutableListOf()
    protected val values: MutableList<Any> = mutableListOf()
    protected val conditions = mutableListOf<String>()
    protected var returnEntity: Boolean = false
    open fun withColumns(vararg columns: String): T {
        this.columns.clear()
        for (t in columns)
            this.columns.add(t)
        return this as T
    }

    open fun withValues(vararg values: Any): T {
        this.values.clear()
        for (t in values)
            this.values.add(t)
        return this as T
    }

    fun andWhere(condition: String) : T {
        conditions.add(condition)
        return this as T
    }

    fun returningEntity() : T {
        returnEntity = true
        return this as T
    }

    protected open fun validate() {
        if (columns.size != values.size)
            throw InvalidQueryStateException("Columns size doesn't match values size" +
                "\ncolumns[${columns.joinToString(", " )}]" +
                "\nvalues[${values.joinToString(", " )}]")
    }

    protected fun convertValueToString(value: Any) : String {
        return when (value) {
            is Long -> value.toString()
            is String -> "'$value'"
            else -> throw Exception("Unknown type")
        }
    }
}