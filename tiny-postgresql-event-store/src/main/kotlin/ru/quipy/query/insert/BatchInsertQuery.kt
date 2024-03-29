package ru.quipy.query.insert

import ru.quipy.query.BasicQuery
import ru.quipy.query.exception.InvalidQueryStateException

class BatchInsertQuery(schema: String, relation: String)
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