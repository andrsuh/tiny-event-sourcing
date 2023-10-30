package jp.veka.query.insert

import java.sql.Connection

class BatchInsertQuery(schema: String, relation: String, private val batchSize: Long) : InsertQuery(schema, relation) {
    private val batches = ArrayList<ArrayList<Any>>()
    override fun withValues(vararg values: Any): BatchInsertQuery {
        val batch = ArrayList<Any>()
        for (value in values) {
            batch.add(value)
        }
        batches.add(batch)
        return this
    }
    override fun withColumns(vararg columns: String): BatchInsertQuery {
        super.withColumns(*columns)
        return this
    }
    override fun execute(connection: Connection): Boolean {
        val prepared = connection.prepareStatement(String.format(
            "insert into %s.%s (%s) values (%s)",
            schema, relation,
            columns.joinToString(","),
            values.joinToString(",") { "?" }
        ))

        for ((count, batch) in batches.withIndex()) {
            prepared.clearParameters()
            super.withValues(batch.toTypedArray())
            super.insertValuesInPreparedStatement(prepared)
            prepared.addBatch()
            if ((count + 1) % batchSize == 0L) {
                prepared.executeBatch()
            }
        }
        return true
    }
}