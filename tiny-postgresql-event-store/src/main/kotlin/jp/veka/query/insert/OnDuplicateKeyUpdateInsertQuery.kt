package jp.veka.query.insert

import java.sql.Connection

class OnDuplicateKeyUpdateInsertQuery
    (schema: String, relation: String)
    : InsertQuery(schema, relation
) {
    private val duplicateKeyUpdateColumns: MutableList<String> = ArrayList()
    fun onDuplicateKeyUpdateValues(vararg columns: String) : OnDuplicateKeyUpdateInsertQuery {
        for (column in columns) {
            duplicateKeyUpdateColumns.add(column)
        }
        return this
    }
    override fun withValues(vararg values: Any): OnDuplicateKeyUpdateInsertQuery {
        super.withValues(*values)
        return this
    }
    override fun withColumns(vararg columns: String): OnDuplicateKeyUpdateInsertQuery {
        super.withColumns(*columns)
        return this
    }
    override fun execute(connection: Connection) : Boolean {
        return connection.prepareStatement(String.format(
            "insert into %s.%s (%s) values (%s)" +
                "on duplicate key update" +
                "set %s",
            schema, relation,
            columns.joinToString(","),
            columns.joinToString(",") { "?" },
            duplicateKeyUpdateColumns.joinToString(",") { "${it}=values(${it})" }))
            .execute()
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