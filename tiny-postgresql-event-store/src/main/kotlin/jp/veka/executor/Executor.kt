package jp.veka.executor

import java.sql.ResultSet

interface Executor {
    fun <T> execute(action: () -> T)
    fun executeReturningResultSet(action: () -> ResultSet): ResultSet?
    fun <T> executeReturningBoolean(action: () -> T): Boolean
}