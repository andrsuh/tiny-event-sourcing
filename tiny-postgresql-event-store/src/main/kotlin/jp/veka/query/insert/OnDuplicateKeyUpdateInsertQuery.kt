package jp.veka.query.insert

import jp.veka.query.BasicQuery
import jp.veka.query.exception.InvalidQueryStateException

class OnDuplicateKeyUpdateInsertQuery(schema: String, relation: String)
    : BasicQuery<OnDuplicateKeyUpdateInsertQuery>(schema, relation) {
    private val duplicateKeyUpdateColumns: MutableList<String> = mutableListOf()
    private val conflictingColumns: MutableList<String> = mutableListOf()
    fun onDuplicateKeyUpdateColumns(vararg columns: String) : OnDuplicateKeyUpdateInsertQuery {
       duplicateKeyUpdateColumns.addAll(columns)
        return this
    }

    fun withPossiblyConflictingColumns(vararg columns: String) : OnDuplicateKeyUpdateInsertQuery {
        conflictingColumns.addAll(columns)
        return this
    }

    override fun validate() {
        super.validate()
        val unknownColumns = duplicateKeyUpdateColumns.filter { !columns.contains(it) }
        if (unknownColumns.isNotEmpty()) {
            throw InvalidQueryStateException(
                "Unknown columns for updating on duplicated key: [${unknownColumns.joinToString { ", " }}]")
        }
    }

    override fun build(): String {
        validate()
        val duplicateColumnToValue = mutableMapOf<String, String>()
        for (column in duplicateKeyUpdateColumns) {
            val value = values[columns.indexOf(column)]
            duplicateColumnToValue[column] = convertValueToString(value)
        }
        var sql =  "insert into ${schema}.${relation} (${columns.joinToString()}) values (${values.joinToString {convertValueToString(it)}}) " +
            "on conflict (${conflictingColumns.joinToString()}) do update " +
            "set ${duplicateColumnToValue.entries.joinToString {"${it.key}=${it.value}"}}"
        if (conditions.isNotEmpty()) {
            sql += " where ${conditions.joinToString(" and ")}"
        }
        return sql
    }
}