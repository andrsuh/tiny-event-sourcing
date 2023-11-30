package jp.veka.config

import jp.veka.db.DataSourceProvider
import jp.veka.db.TestDataSourceProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class PostgresEventStoreTestConfiguration {
    @Bean
    fun dataSourceProvider(
        @Value("\${jdbc.dbName:}") dbName: String,
        @Value("\${jdbc.username:}") username: String,
        @Value("\${jdbc.password:}") password: String,
        @Value("\${schema:event_sourcing_store}") schema: String)
        : DataSourceProvider {
        return TestDataSourceProvider(dbName, username, password, schema)
    }
}