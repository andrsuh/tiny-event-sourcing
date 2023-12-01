package jp.veka.query

import jp.veka.query.insert.InsertQuery
import jp.veka.query.insert.OnDuplicateKeyUpdateInsertQuery
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class InsertQueryTest {
    private val schema = "schema"
    private val relation = "relation"
    private val columns = arrayOf("a", "b", "c")
    private val onDuplicateKeyUpdateColumns = arrayOf("a", "b")
    private val values = arrayOf("a1", "b1", "c1")
    @Test
    fun testSimpleInsertQuery() {
        val query = InsertQuery(schema, relation)
            .withColumns(columns = columns)
            .withValues(values = values)

        Assertions.assertEquals(query.getTemplateSql(),
            String.format("insert into %s.%s (%s) values (%s)",
                schema, relation, columns.joinToString(), values.joinToString { "?" })
        )
    }

    @Test
    fun testOnDuplicateKeyUpdateQuery() {
        val query = OnDuplicateKeyUpdateInsertQuery(schema, relation)
            .withColumns(columns = columns)
            .onDuplicateKeyUpdateColumns(columns = onDuplicateKeyUpdateColumns)

        Assertions.assertEquals(query.getTemplateSql(),
            String.format("insert into %s.%s (%s) values (%s) on conflict (%s) do update set %s", schema, relation,
                columns.joinToString(),
                columns.joinToString { "?" },
                onDuplicateKeyUpdateColumns.joinToString(),
                onDuplicateKeyUpdateColumns.joinToString { "$it=?" })
        )
    }

    // TODO append tests for every query and case
}