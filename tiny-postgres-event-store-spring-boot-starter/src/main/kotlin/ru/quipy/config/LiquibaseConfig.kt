package ru.quipy.config

import liquibase.integration.spring.SpringLiquibase
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import java.sql.SQLException
import javax.sql.DataSource

@Configuration
class LiquibaseConfig {
    @Bean
    @Primary
    @ConditionalOnBean(DataSource::class)
    fun liquibaseTinyEs(dataSource: DataSource,
        @Value("\${schema:event_sourcing_store}") schema: String): SpringLiquibase {
        try {
            dataSource.connection
                .createStatement()
                .execute("CREATE SCHEMA IF NOT EXISTS $schema;")
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
        val liquibase = SpringLiquibase()
        liquibase.changeLog = "classpath:liquibase/changelog.sql"
        liquibase.dataSource = dataSource
        liquibase.defaultSchema = schema
        liquibase.setChangeLogParameters(
            mapOf(
                Pair("database.liquibaseSchemaName", schema),
                Pair("database.defaultSchemaName", schema)
            )
        )
        return liquibase
    }
}