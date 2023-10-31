package jp.veka.autoconfigure

import jp.veka.PostgresClientEventStore
import jp.veka.PostgresTemplateEventStore
import jp.veka.factory.PostgresConnectionFactory
import jp.veka.factory.PostgresConnectionFactoryImpl
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.database.EventStore

@Configuration
class PostgresEventStoreAutoconfiguration {
    @Bean
    fun postgresTemplateEventStore(): EventStore = PostgresTemplateEventStore()

    @Bean("postgresConnectionFactory")
    fun databaseFactory(
        @Value("\${jdbc.connectionString})") jdbcUrl: String,
        @Value("\${jdbc.username})") username: String,
        @Value("\${jdbc.password})") password: String
    ) : PostgresConnectionFactory {
        return PostgresConnectionFactoryImpl(jdbcUrl, username, password)
    }
    @Bean
    fun postgresClientEventStore(
        @Qualifier("postgresConnectionFactory")databaseFactory: PostgresConnectionFactory,
        @Value("\${schema:event_sourcing_store})") schema: String
    ): EventStore {
        return PostgresClientEventStore(databaseFactory)
    }
}