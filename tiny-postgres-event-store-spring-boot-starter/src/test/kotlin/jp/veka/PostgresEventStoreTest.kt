package jp.veka

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import ru.quipy.database.EventStore

@SpringBootTest(
    classes = [PostgresEventStoreTestConfiguration::class]
)
@ActiveProfiles("test")
class PostgresEventStoreTest {

    @Autowired
    @Qualifier("postgresClientEventStore")
    private lateinit var postgresClientEventStore : EventStore

    @Autowired
    @Qualifier("postgresTemplateEventStore")
    private lateinit var postgresTemplateEventStore: EventStore


    @Test
    fun test() {
        
    }
}