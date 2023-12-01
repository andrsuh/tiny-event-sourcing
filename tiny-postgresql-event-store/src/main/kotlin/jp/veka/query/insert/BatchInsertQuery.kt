package jp.veka.query.insert

import jp.veka.query.BasicQuery
import jp.veka.query.exception.InvalidQueryStateException
import java.sql.Connection

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

    fun getTemplate() : String {
        return String.format(
            "insert into %s.%s (%s) values (%s)",
            schema, relation,
            columns.joinToString(),
            columns.joinToString {"?"}
        )
    }
    override fun validate() {
        for (batch in batches) {
            if (batch.size != columns.size) {
                throw InvalidQueryStateException("Columns size doesn't match batch values size" +
                    "\ncolumns[${columns.joinToString( )}]" +
                    "\nvalues[${values.joinToString()}]")
            }
        }
    }
    override fun build(): String {
        validate()
        val resultQueries = mutableListOf<String>()
        for (batch in batches) {
            super.withValues(values = batch.toTypedArray())
            resultQueries.add(String.format(
                "insert into %s.%s (%s) values (%s)",
                schema, relation,
                columns.joinToString(),
                values.joinToString {convertValueToString(it)}
            ))
        }

        return resultQueries.joinToString(";\n")
    }
}