package ru.quipy

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import ru.quipy.tables.EventRecordTable
import ru.quipy.tables.EventStreamActiveReadersTable
import ru.quipy.tables.EventStreamReadIndexTable
import ru.quipy.tables.SnapshotTable

@SpringBootTest
class BaseTest(private val testId: String) {
    // @Autowired
    // lateinit var mongoTemplate: MongoTemplate

    @Value("\${defaultSchema:event_sourcing_store}")
    private lateinit var schema: String

    @Autowired
    private lateinit var databaseConnectionFactory: ru.quipy.db.factory.ConnectionFactory
    fun cleanDatabase() {
        // mongoTemplate.remove(Query.query(Criteria.where("aggregateId").`is`(testId)), "aggregate-project")
        // mongoTemplate.remove(Query.query(Criteria.where("_id").`is`(testId)), "snapshots")

        databaseConnectionFactory.getDatabaseConnection().prepareStatement(
            "truncate ${schema}.${EventRecordTable.name};" +
                "truncate ${schema}.${SnapshotTable.name};" +
                "truncate ${schema}.${EventStreamReadIndexTable.name};" +
                "truncate ${schema}.${EventStreamActiveReadersTable.name};"
        ).execute()
    }
}