package ru.quipy.executor

import org.slf4j.Logger
import ru.quipy.db.factory.ConnectionFactory
import ru.quipy.query.BasicQuery
import ru.quipy.query.Query
import ru.quipy.query.insert.BatchInsertQuery
import java.sql.ResultSet
import java.sql.SQLException

open class ExceptionLoggingSqlQueriesExecutor(
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
        executeDependingOnQueryType(query)
    }

    override fun <T: Query, E> executeAndProcessResultSet(query: BasicQuery<T>,
        processFunction: (ResultSet?) -> E?): E? {
        if (query is BatchInsertQuery) {
            throw UnsupportedOperationException("Cannot return result set executing batch insert")
        }
        return connectionFactory.getDatabaseConnection().use { connection ->
            processFunction(
                connection.prepareStatement(query.build()).executeQuery()
            )
        }
    }

    open fun <T: Query> executeDependingOnQueryType(query: BasicQuery<T>) {
        when (query) {
            is BatchInsertQuery -> executeBatchInsert(query)
            else -> {
                connectionFactory.getDatabaseConnection().use { connection ->
                    connection.prepareStatement(query.build())
                    .execute()
                }
            }
        }
    }

    open fun executeBatchInsert(query: BatchInsertQuery) {
        var sqls = query.build().split("\n;")
        connectionFactory.getDatabaseConnection().use { connection ->
            val prepared = connection.createStatement()
            for ((count, sql) in sqls.withIndex()) {
                prepared.addBatch(sql)
                if ((count + 1) % batchInsertSize.toLong() == 0L) {
                    prepared.executeBatch()
                }
            }
            prepared.executeBatch()
        }
    }
}