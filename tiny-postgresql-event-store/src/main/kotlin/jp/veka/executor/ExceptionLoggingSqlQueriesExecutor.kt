package jp.veka.executor

import org.slf4j.Logger
import java.sql.ResultSet
import java.sql.SQLException

class ExceptionLoggingSqlQueriesExecutor(private val logger: Logger) : Executor {
    override fun <T> executeReturningBoolean(action: () -> T): Boolean {
        return try {
            action()
            true
        } catch (ex: SQLException) {
            logger.error(ex.message)
            false
        }
    }

    override fun <T> execute(action: () -> T) {
        try {
            action()
        } catch (ex: SQLException) {
            logger.error(ex.message)
        }
    }

    override fun executeReturningResultSet(action: () -> ResultSet): ResultSet? {
        return try {
            action()
        } catch (ex: SQLException) {
            logger.error(ex.message)
            null
        }
    }
}