package jp.veka.query.update

import jp.veka.query.Query
import java.sql.Connection
import java.sql.ResultSet

class UpdateQuery : Query {
    private val schema: String
    private val relation: String
    private var columnValueMap = mutableMapOf<String, Any>()
    private val conditions = mutableListOf<String>()
    private var returnEntity: Boolean = false

    constructor(schema: String, relation: String) {
        this.schema = schema
        this.relation = relation
        this.columnValueMap = HashMap()
    }

    fun andWhere(condition: String) : UpdateQuery {
        conditions.add(condition)
        return this
    }

    fun set(column: String, value: Any) : UpdateQuery {
        columnValueMap[column] = value
        return this
    }

    fun withReturningEntity() : UpdateQuery {
        returnEntity = true
        return this
    }
    override fun execute(connection: Connection): ResultSet {
        var sql  = String.format(
            "update %s.%s set %s where %s",
            schema,
            relation,
            columnValueMap.map { "${it.key} = ${it.value}" }.joinToString { "," },
            conditions.joinToString { " and " }
        )

        if (returnEntity) {
            sql = "$sql returning *"
        }

        return connection.prepareStatement(sql)
            .executeQuery()
    }
}