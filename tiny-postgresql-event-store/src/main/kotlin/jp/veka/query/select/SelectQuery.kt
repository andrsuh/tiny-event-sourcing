package jp.veka.query.select

import jp.veka.query.BasicQuery
import java.sql.Connection
import java.sql.ResultSet

class SelectQuery(schema: String, relation: String) : BasicQuery<SelectQuery>(schema, relation) {
    private var limit: Int = -1
    fun limit(limit: Int) : SelectQuery {
        if (limit > 0) {
            this.limit = limit
        }
        return this
    }

    override fun execute(connection: Connection) : ResultSet {
        validate()
        var sql = String.format("select * from ${schema}.${relation} where %s",
            conditions.joinToString { " and " })
        if (limit > 0) sql = "$sql limit $limit"
        return connection.createStatement().executeQuery(sql)
    }
}