package jp.veka.executor

import jp.veka.query.Query
import java.sql.ResultSet

interface QueryExecutor {
    fun execute(query: Query)
    fun executeReturningResultSet(query: Query): ResultSet?
    fun executeReturningBoolean(query: Query): Boolean
}