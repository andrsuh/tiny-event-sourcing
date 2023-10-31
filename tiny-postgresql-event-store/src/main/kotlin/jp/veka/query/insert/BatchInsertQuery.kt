package jp.veka.query.insert

import jp.veka.query.BasicQuery
import java.lang.Exception
import java.sql.Connection
import java.sql.PreparedStatement

class BatchInsertQuery(schema: String, relation: String, private val batchSize: Long)
    : BasicQuery<BatchInsertQuery>(schema, relation) {
    private val batches = ArrayList<ArrayList<Any>>()
    override fun withValues(vararg values: Any): BatchInsertQuery {
        val batch = ArrayList<Any>()
        for (value in values) {
            batch.add(value)
        }
        batches.add(batch)
        return this
    }

    override fun execute(connection: Connection): Boolean {
        validate()
        val prepared = connection.prepareStatement(String.format(
            "insert into %s.%s (%s) values (%s)",
            schema, relation,
            columns.joinToString(","),
            values.joinToString(",") { "?" }
        ))

        for ((count, batch) in batches.withIndex()) {
            prepared.clearParameters()
            super.withValues(batch.toTypedArray())
            insertValuesInPreparedStatement(prepared)
            prepared.addBatch()
            if ((count + 1) % batchSize == 0L) {
                prepared.executeBatch()
            }
        }
        return true
    }


    private fun insertValuesInPreparedStatement(ps: PreparedStatement) {
        validate()
        for ((i, value) in values.withIndex()) {
            when (value) {
                is Long -> ps.setLong(i + 1, value)
                is String -> ps.setString(i + 1, value)
                else -> throw Exception("Unknown type")
            }
        }
    }
}