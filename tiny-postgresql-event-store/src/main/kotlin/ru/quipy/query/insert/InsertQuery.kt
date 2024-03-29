package ru.quipy.query.insert

import ru.quipy.query.BasicQuery
import java.sql.Connection

open class InsertQuery(schema: String, relation: String) : BasicQuery<InsertQuery>(schema, relation) {
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