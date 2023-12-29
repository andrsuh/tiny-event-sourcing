package ru.quipy.config

import org.flywaydb.core.Flyway
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import javax.sql.DataSource

@Configuration
class FlywayConfig {
    @Primary
    @Bean(initMethod = "migrate")
    @ConditionalOnBean(DataSource::class)
    fun flyway(dataSource: DataSource,
        @Value("\${schema:event_sourcing_store}") schema: String) : Flyway {
        return Flyway.configure()
            .locations("/migrations")
            .dataSource(dataSource)
            .defaultSchema(schema)
            .load()
    }
}