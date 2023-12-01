package jp.veka.query.insert

import jp.veka.query.BasicQuery
import jp.veka.query.exception.InvalidQueryStateException
import java.sql.Connection
import java.sql.PreparedStatement

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
    override fun getTemplateSql(): String {
        var sql =  "insert into ${schema}.${relation} (${columns.joinToString()}) values (${columns.joinToString {"?"}}) " +
            "on conflict (${conflictingColumns.joinToString()}) do update " +
            "set ${duplicateKeyUpdateColumns.joinToString() {"${it}=?"}}"
        if (conditions.isNotEmpty()) {
            sql += " where ${conditions.joinToString(" and ")}"
        }
        return sql
    }

    override fun validate() {
        super.validate()
        val unknownColumns = duplicateKeyUpdateColumns.filter { !columns.contains(it) }
        if (unknownColumns.isNotEmpty()) {
            throw InvalidQueryStateException(
                "Unknown columns for updating on duplicated key: [${unknownColumns.joinToString { ", " }}]")
        }
    }

    override fun insertValuesInPreparedStatement(ps: PreparedStatement) {
        super.insertValuesInPreparedStatement(ps)
        for ((i, column) in duplicateKeyUpdateColumns.withIndex()) {
            var value = values[columns.indexOf(column)]
            var index = columns.size + i + 1
            insertValueInPreparedStatement(index, value, ps)
        }
    }

    override fun execute(connection: Connection) : Boolean {
        var ps = connection.prepareStatement(getTemplateSql())
        insertValuesInPreparedStatement(ps)
        return ps.execute()
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