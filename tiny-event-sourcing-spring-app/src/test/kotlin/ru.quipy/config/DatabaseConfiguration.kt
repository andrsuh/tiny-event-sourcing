package ru.quipy.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

@Configuration
class DatabaseConfiguration {
    @Bean
    fun dataSource(
        @Value("\${jdbc.dbName:tiny_es}") dbName: String,
        @Value("\${jdbc.username:tiny_es}") username: String,
        @Value("\${jdbc.password:tiny_es}") password: String,
        @Value("\${schema:event_sourcing_store}") schema: String)
        : HikariDataSource {
        val container = PostgreSQLContainer(DockerImageName.parse("postgres:14.9-alpine")).apply {
            withDatabaseName(dbName)
            withUsername(username)
            withPassword(password)
        }
        if (!container.isRunning) {
            container.start()
        }
        val hikariConfig = HikariConfig()
        hikariConfig.maximumPoolSize = 20
        hikariConfig.idleTimeout = 30000
        hikariConfig.jdbcUrl = container.jdbcUrl
        hikariConfig.username = username
        hikariConfig.password = password

        return HikariDataSource(hikariConfig)
    }
}