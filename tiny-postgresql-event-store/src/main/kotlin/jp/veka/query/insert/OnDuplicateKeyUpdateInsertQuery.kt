package jp.veka.query.insert

import jp.veka.query.BasicQuery
import jp.veka.query.exception.InvalidQueryStateException
import java.sql.Connection

class OnDuplicateKeyUpdateInsertQuery(schema: String, relation: String)
    : BasicQuery<OnDuplicateKeyUpdateInsertQuery>(schema, relation) {
    private val duplicateKeyUpdateColumns: MutableList<String> = mutableListOf()
    fun onDuplicateKeyUpdateValues(vararg columns: String) : OnDuplicateKeyUpdateInsertQuery {
        for (column in columns) {
            duplicateKeyUpdateColumns.add(column)
        }
        return this
    }
    override fun execute(connection: Connection) : Boolean {
        return connection.prepareStatement(
            "insert into ${schema}.${relation} (${columns.joinToString(",")}) values (${ values.joinToString(",")})" +
                "on duplicate key update" +
                "set ${ duplicateKeyUpdateColumns.joinToString(",") { "${it}=values(${it})" }}")
            .execute()
    }

    override fun validate() {
        super.validate()
        val unknownColumns = duplicateKeyUpdateColumns.filter { !columns.contains(it) }
        if (unknownColumns.isNotEmpty()) {
            throw InvalidQueryStateException(
                "Unknown columns for updating on duplicated key: [${unknownColumns.joinToString { ", " }}]")
        }
    }
}