package ru.quipy.query.select

import ru.quipy.query.BasicQuery
import java.sql.Connection
import java.sql.ResultSet

class SelectQuery(schema: String, relation: String) : BasicQuery<SelectQuery>(schema, relation) {
    private var limit: Int = -1
    fun limit(limit: Int) : SelectQuery {
        if (limit > 0) {
            this.limit = limit
        }
        return this
    }

    override fun build(): String {
        // no need to validate query state
        var columnsStr = "*"
        if (columns.isNotEmpty()) {
            columnsStr = columns.joinToString()
        }
        var sql = String.format("select $columnsStr from ${schema}.${relation}")
        if (conditions.isNotEmpty()) {
            sql += " where ${conditions.joinToString( " and " )}"
        }
        if (limit > 0) sql = "$sql limit $limit"
        return sql
    }

    override fun withValues(vararg values: Any): SelectQuery {
        throw UnsupportedOperationException()
    }
}