package jp.veka.query.delete

import jp.veka.query.BasicQuery
import java.sql.Connection

class DeleteQuery(schema:String, relation: String) : BasicQuery<DeleteQuery>(schema, relation) {
    override fun build(): String {
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
}