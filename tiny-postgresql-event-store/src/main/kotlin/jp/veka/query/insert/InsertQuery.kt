package jp.veka.query.insert

import jp.veka.query.Query
import java.lang.Exception
import java.sql.Connection
import java.sql.PreparedStatement

open class InsertQuery(protected val schema: String, protected val relation: String) : Query {
    protected val columns: MutableList<String> = mutableListOf()
    protected val values: MutableList<Any> = mutableListOf()

    open fun withColumns(vararg columns: String) : InsertQuery {
        this.columns.clear()
        for (t in columns)
            this.columns.add(t)
        return this
    }

    open fun withValues(vararg values: Any) : InsertQuery {
        this.values.clear()
        for (t in values)
            this.values.add(t)
        return this
    }

    override fun execute(connection: Connection): Boolean {
        return connection.prepareStatement(String.format(
            "insert into %s.%s (%s) values (%s)",
            schema, relation,
            columns.joinToString(","),
            values.joinToString(",")
        )).execute()
    }

    protected fun insertValuesInPreparedStatement(ps: PreparedStatement) {
        var i = 0;
        for (value in values) {
            when (value) {
                is Long -> ps.setLong(++i, value)
                is String -> ps.setString(++i, value)
                else -> throw Exception("Unknown type")
            }
        }
    }
}