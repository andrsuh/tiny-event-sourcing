package jp.veka.query.update

import jp.veka.query.BasicQuery
import java.sql.Connection
import java.sql.ResultSet

class UpdateQuery(schema: String, relation: String) : BasicQuery<UpdateQuery>(schema, relation) {
    private var columnValueMap = mutableMapOf<String, Any>()

    fun set(column: String, value: Any) : UpdateQuery {
        columnValueMap[column] = value
        return this
    }

    override fun build(): String {
        validate()
        var sql  = String.format(
            "update %s.%s set %s where %s",
            schema,
            relation,
            columnValueMap.map { "${it.key} = ${convertValueToString(it.value)}" }.joinToString(),
            conditions.joinToString(" and ")
        )

        if (returnEntity) {
            sql = "$sql returning *"
        }
        return sql
    }
}