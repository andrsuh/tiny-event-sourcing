package jp.veka.query

import jp.veka.query.select.SelectQuery
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class SelectQueryTest {
    private val schema = "schema"
    private val relation = "relation"
    private val columns = arrayOf("a", "b", "c")
    @Test
    fun testSelectQuerySqlWithoutConditionsWithColumnsAndLimit() {
        val limit = 101;
        val query = SelectQuery(schema, relation)
            .withColumns(columns = columns)
            .limit(limit)

        Assertions.assertEquals(query.getTemplateSql(),
            String.format("select %s from %s.%s limit %d", columns.joinToString(), schema, relation, limit)
        )
    }

    @Test
    fun testSelectQuerySqlWithConditionsWithoutColumnsAndNoLimit() {
        val query = SelectQuery(schema, relation)
            .andWhere("a = b")
            .andWhere("b > c")

        Assertions.assertEquals(query.getTemplateSql(),
            String.format("select * from %s.%s where a = b and b > c", schema, relation)
        )
    }
}