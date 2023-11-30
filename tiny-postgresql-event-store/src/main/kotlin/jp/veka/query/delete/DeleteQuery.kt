package jp.veka.query.delete

import jp.veka.query.BasicQuery
import java.sql.Connection

class DeleteQuery(schema:String, relation: String) : BasicQuery<DeleteQuery>(schema, relation) {
    override fun getTemplateSql(): String {
        var sql  = String.format(
            "delete from %s.%s where %s",
            schema,
            relation,
            conditions.joinToString { " and " }
        )

        if (returnEntity) {
            sql = "$sql returning *"
        }
        return sql
    }

    override fun execute(connection: Connection): Any {
        validate()
        return connection.prepareStatement(getTemplateSql())
            .executeQuery()
    }
}