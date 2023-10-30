package jp.veka.query.select

import jp.veka.query.Query
import java.sql.Connection
import java.sql.ResultSet

class SelectQuery : Query {
    private val schema: String
    private val relation: String
    private val conditions = mutableListOf<String>()
    private var limit: Int = -1
    constructor(schema: String, relation: String) {
        this.schema = schema
        this.relation = relation
    }
    fun andWhere(condition: String) : SelectQuery {
        conditions.add(condition)
        return this
    }

    fun limit(limit: Int) : SelectQuery {
        if (limit > 0) {
            this.limit = limit
        }
        return this
    }

    override fun execute(connection: Connection) : ResultSet {
        return connection.createStatement().executeQuery(getQueryWithLimit())
    }

    private fun getQueryWithLimit() : String {
        var sql = String.format("select * from ${schema}.${relation} where %s",
            conditions.joinToString { " and " })
        if (limit <= 0) return sql
        return "$sql limit $limit"
    }
}