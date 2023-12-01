package jp.veka.query.insert

import jp.veka.query.BasicQuery
import java.sql.Connection

open class InsertQuery(schema: String, relation: String) : BasicQuery<InsertQuery>(schema, relation) {
    override fun getTemplateSql(): String {
        return String.format(
            "insert into %s.%s (%s) values (%s)",
            schema, relation,
            columns.joinToString(),
            columns.joinToString{"?"}
        )
    }

    override fun execute(connection: Connection): Boolean {
        validate()
        val preparedStatement = connection.prepareStatement(getTemplateSql())
        insertValuesInPreparedStatement(preparedStatement)
        return preparedStatement.execute()
    }

    override fun build(): String {
        validate()
        return String.format(
            "insert into %s.%s (%s) values (%s)",
            schema, relation,
            columns.joinToString(),
            values.joinToString { convertValueToString(it) }
        )
    }
}