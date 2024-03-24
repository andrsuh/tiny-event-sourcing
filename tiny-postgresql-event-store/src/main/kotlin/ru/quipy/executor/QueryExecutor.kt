package ru.quipy.executor

import ru.quipy.query.BasicQuery
import ru.quipy.query.Query
import java.sql.ResultSet

interface QueryExecutor {
    fun <T: Query> execute(query: BasicQuery<T>)
    fun <T: Query, E> executeAndProcessResultSet(query: BasicQuery<T>, processFunction: (ResultSet?) -> E?): E?
    fun <T: Query> executeReturningBoolean(query: BasicQuery<T>): Boolean
}