package ru.quipy.config

import liquibase.integration.spring.SpringLiquibase
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.core.io.FileSystemResourceLoader
import java.sql.SQLException
import javax.sql.DataSource

@Configuration
class LiquibaseConfig {
    @Bean
    @Primary
    @ConditionalOnBean(DataSource::class)
    fun liquibaseTinyEs(dataSource: DataSource,
        @Value("\${tiny-es.storage.schema:event_sourcing_store}") schema: String): SpringLiquibase {
        try {
            dataSource.connection.use { connection ->
                connection.createStatement()
                .execute("CREATE SCHEMA IF NOT EXISTS $schema;")
            }
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
        val liquibase = SpringLiquibase()
        liquibase.resourceLoader = FileSystemResourceLoader()
        liquibase.liquibaseSchema = schema
        liquibase.defaultSchema = schema
        liquibase.changeLog = "classpath:liquibase/changelog.sql"
        liquibase.dataSource = dataSource
        liquibase.setChangeLogParameters(
            mapOf(
                Pair("schema", schema)
            )
        )
        return liquibase
    }
}