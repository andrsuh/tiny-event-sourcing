package ru.quipy.executor

import ru.quipy.db.factory.ConnectionFactory
import ru.quipy.query.BasicQuery
import ru.quipy.query.Query
import ru.quipy.query.insert.BatchInsertQuery
import ru.quipy.query.insert.InsertQuery
import ru.quipy.query.insert.OnDuplicateKeyUpdateInsertQuery
import ru.quipy.query.select.SelectQuery
import org.slf4j.Logger
import java.sql.ResultSet
import java.sql.SQLException

class ExceptionLoggingSqlQueriesExecutor(
    private val connectionFactory: ConnectionFactory,
    private val batchInsertSize: Int,
    private val logger: Logger) : QueryExecutor {
    override fun <T: Query> executeReturningBoolean(query: BasicQuery<T>): Boolean {
        return try {
            executeDependingOnQueryType(query)
            true
        } catch (ex: SQLException) {
            logger.error(ex.message)
            false
        }
    }

    override fun <T: Query> execute(query: BasicQuery<T>) {
        try {
            executeDependingOnQueryType(query)
        } catch (ex: SQLException) {
            logger.error(ex.message)
        }
    }

    override fun <T: Query> executeReturningResultSet(query: BasicQuery<T>): ResultSet? {
        return try {
            if (query is BatchInsertQuery) {
                throw UnsupportedOperationException("Cannot return result set executing batch insert")
            }
            connectionFactory.getDatabaseConnection().prepareStatement(query.build())
                .executeQuery()
        } catch (ex: SQLException) {
            logger.error(ex.message)
            null
        }
    }


    private fun <T: Query> executeDependingOnQueryType(query: BasicQuery<T>) {
        when (query) {
            is BatchInsertQuery -> executeBatchInsert(query)
            else -> connectionFactory.getDatabaseConnection().prepareStatement(query.build())
                .execute()
        }
    }

    private fun executeBatchInsert(query: BatchInsertQuery) {
        var sqls = query.build().split("\n;")
        val prepared = connectionFactory.getDatabaseConnection().createStatement()
        for ((count, sql) in sqls.withIndex()) {
            prepared.addBatch(sql)
            if ((count + 1) % batchInsertSize.toLong() == 0L) {
                prepared.executeBatch()
            }
        }
        prepared.executeBatch()
    }
}