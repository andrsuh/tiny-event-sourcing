package ru.quipy.query.select

import ru.quipy.query.BasicQuery
import ru.quipy.query.exception.InvalidQueryStateException

class SelectQuery(schema: String, relation: String) : BasicQuery<SelectQuery>(schema, relation) {
    private var limit: Int = -1
    private var orderByColumn: String? = null
    private var sortingOrder: SortingOrder = SortingOrder.ASCENDING
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
        if (orderByColumn != null) sql += " order by $orderByColumn ${sortingOrder.sqlName}"
        if (limit > 0) sql = "$sql limit $limit"
        return sql
    }

    override fun withValues(vararg values: Any): SelectQuery {
        throw UnsupportedOperationException()
    }

    fun orderBy(column: String, mode: SortingOrder = SortingOrder.ASCENDING): SelectQuery {
        orderByColumn = column
        sortingOrder = sortingOrder
        return this
    }

    override fun validate() {
        super.validate()
        if (orderByColumn != null && !columns.contains(orderByColumn)) {
            throw InvalidQueryStateException("Colunm $orderByColumn is not present in select columns " +
                "[${columns.joinToString(", " )}]")
        }
    }

    enum class SortingOrder(val sqlName: String) {
        ASCENDING("asc"),
        DESCENDING("desc");
    }
}