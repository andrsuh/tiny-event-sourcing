package jp.veka.executor

import jp.veka.db.factory.ConnectionFactory
import jp.veka.query.Query
import org.slf4j.Logger
import java.sql.ResultSet
import java.sql.SQLException

class ExceptionLoggingSqlQueriesExecutor(private val connectionFactory: ConnectionFactory, private val logger: Logger) : QueryExecutor {
    override fun executeReturningBoolean(query: Query): Boolean {
        return try {
            connectionFactory.getDatabaseConnection().prepareStatement(query.build())
                .execute()
            true
        } catch (ex: SQLException) {
            logger.error(ex.message)
            false
        }
    }

    override fun execute(query: Query) {
        try {
            connectionFactory.getDatabaseConnection().prepareStatement(query.build())
                .execute()
        } catch (ex: SQLException) {
            logger.error(ex.message)
        }
    }

    override fun executeReturningResultSet(query: Query): ResultSet? {
        return try {
            connectionFactory.getDatabaseConnection().prepareStatement(query.build())
                .executeQuery()
        } catch (ex: SQLException) {
            logger.error(ex.message)
            null
        }
    }
}