package jp.veka.executor

import jp.veka.db.factory.ConnectionFactory
import jp.veka.query.BasicQuery
import jp.veka.query.Query
import jp.veka.query.insert.BatchInsertQuery
import jp.veka.query.insert.InsertQuery
import jp.veka.query.insert.OnDuplicateKeyUpdateInsertQuery
import jp.veka.query.select.SelectQuery
import org.slf4j.Logger
import java.sql.ResultSet
import java.sql.SQLException

class ExceptionLoggingSqlQueriesExecutor(
    private val connectionFactory: ConnectionFactory,
    private val batchInsertSize: Long,
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
            if ((count + 1) % batchInsertSize == 0L) {
                prepared.executeBatch()
            }
        }
        prepared.executeBatch()
    }
}