package jp.veka.executor

import org.slf4j.Logger
import java.sql.ResultSet
import java.sql.SQLException

class ExceptionLoggingSqlQueriesExecutor(private val logger: Logger) {
    fun <T> executeReturningBoolean(action: () -> T): Boolean {
        return try {
            action()
            true
        } catch (ex: SQLException) {
            logger.error(ex.message)
            false
        }
    }

    fun <T> execute(action: () -> T) {
        try {
            action()
        } catch (ex: SQLException) {
            logger.error(ex.message)
        }
    }

    fun executeReturningResultSet(action: () -> ResultSet): ResultSet? {
        return try {
            action()
        } catch (ex: SQLException) {
            logger.error(ex.message)
            null
        }
    }
}