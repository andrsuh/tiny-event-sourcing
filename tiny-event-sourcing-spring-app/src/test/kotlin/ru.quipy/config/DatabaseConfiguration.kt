package ru.quipy.config

import org.postgresql.ds.PGSimpleDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import javax.sql.DataSource

@Configuration
class DatabaseConfiguration {
    @Bean
    fun dataSource(
        @Value("\${jdbc.dbName:tiny_es}") dbName: String,
        @Value("\${jdbc.username:tiny_es}") username: String,
        @Value("\${jdbc.password:tiny_es}") password: String,
        @Value("\${schema:event_sourcing_store}") schema: String)
        : DataSource {
        val container = PostgreSQLContainer(DockerImageName.parse("postgres:14.9-alpine")).apply {
            withDatabaseName(dbName)
            withUsername(username)
            withPassword(password)
        }
        if (!container.isRunning) {
            container.start()
        }
        return PGSimpleDataSource().apply {
            setURL(container.jdbcUrl)
            user = username
            this.password = password
        }
    }
}