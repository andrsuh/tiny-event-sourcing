package ru.quipy.query

import ru.quipy.query.insert.InsertQuery
import ru.quipy.query.insert.OnDuplicateKeyUpdateInsertQuery
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

        Assertions.assertEquals(query.build(),
            "insert into schema.relation (a, b, c) values ('a1', 'b1', 'c1')"
        )
    }

    @Test
    fun testOnDuplicateKeyUpdateQuery() {
        val query = OnDuplicateKeyUpdateInsertQuery(schema, relation)
            .withColumns(columns = columns)
            .withValues(values = columns)
            .withPossiblyConflictingColumns("c")
            .onDuplicateKeyUpdateColumns(columns = onDuplicateKeyUpdateColumns)

        Assertions.assertEquals(query.build(),
            "insert into schema.relation (a, b, c) values ('a', 'b', 'c')" +
                " on conflict (c) do update set a='a', b='b'"
        )
    }

    // TODO append tests for every query and case
}