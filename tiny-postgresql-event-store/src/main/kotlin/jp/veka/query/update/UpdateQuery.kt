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

    override fun getTemplateSql(): String {
        var sql  = String.format(
            "update %s.%s set %s where %s",
            schema,
            relation,
            columnValueMap.map { "${it.key} = ?" }.joinToString { ", " },
            conditions.joinToString(" and ")
        )

        if (returnEntity) {
            sql = "$sql returning *"
        }
        return sql
    }
    override fun execute(connection: Connection): ResultSet {
        validate()
        var ps = connection.prepareStatement(getTemplateSql())
        insertValuesInPreparedStatement(ps)
        return ps.executeQuery()
    }
}