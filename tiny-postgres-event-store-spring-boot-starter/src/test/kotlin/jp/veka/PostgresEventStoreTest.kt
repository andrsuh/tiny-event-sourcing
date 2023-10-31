package jp.veka

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import ru.quipy.database.EventStore

@SpringBootTest(
    classes = [PostgresEventStoreTestConfiguration::class]
)
@EnableAutoConfiguration
@ActiveProfiles("test")
class PostgresEventStoreTest {

    @Autowired
    private lateinit var postgresClientEventStore : EventStore

    @Autowired
    private lateinit var postgresTemplateEventStore: EventStore


    @Test
    fun test() {
        
    }
}