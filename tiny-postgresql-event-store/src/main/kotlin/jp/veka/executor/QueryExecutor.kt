package jp.veka.executor

import jp.veka.query.BasicQuery
import jp.veka.query.Query
import java.sql.ResultSet

interface QueryExecutor {
    fun <T: Query> execute(query: BasicQuery<T>)
    fun <T: Query> executeReturningResultSet(query: BasicQuery<T>): ResultSet?
    fun <T: Query> executeReturningBoolean(query: BasicQuery<T>): Boolean
}