package ru.quipy.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

@Configuration
class DatabaseConfig {
    @Bean
    @ConditionalOnProperty("jdbc.connectionString")
    fun dataSource(@Value("\${jdbc.connectionString:}") databaseUrl: String,
        @Value("\${jdbc.username:}") username: String,
        @Value("\${jdbc.password:}") password: String): DataSource {
        val hikariConfig = HikariConfig()
        hikariConfig.maximumPoolSize = 20
        hikariConfig.idleTimeout = 30000
        hikariConfig.jdbcUrl = databaseUrl
        hikariConfig.username = username
        hikariConfig.password = password

        return HikariDataSource(hikariConfig)
    }
}