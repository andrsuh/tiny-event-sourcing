package ru.quipy.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary

@Configuration
class DatabaseConfig {
    @Bean
    @Primary
    @ConditionalOnProperty("spring.datasource.hikari.jdbc-url")
    fun dataSource(@Value("\${spring.datasource.hikari.jdbc-url}") databaseUrl: String,
        @Value("\${spring.datasource.hikari.username:}") username: String,
        @Value("\${spring.datasource.hikari.password:}") password: String,
        @Value("\${spring.datasource.hikari.idleTimeout:30000}") idleTimeout: Long,
        @Value("\${spring.datasource.hikari.maximumPoolSize:20}") maxPoolSize: Int): HikariDataSource {
        val hikariConfig = HikariConfig()
        hikariConfig.maximumPoolSize = maxPoolSize
        hikariConfig.idleTimeout = idleTimeout
        hikariConfig.jdbcUrl = databaseUrl
        hikariConfig.username = username
        hikariConfig.password = password

        return HikariDataSource(hikariConfig)
    }
}