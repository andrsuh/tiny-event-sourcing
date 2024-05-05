package ru.quipy.config

import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.util.Properties

class DockerPostgresDataSourceInitializer {
    private val postgresContainer = PostgreSQLContainer(DockerImageName.parse("postgres:14.9-alpine")).apply {
        withDatabaseName("tiny_es")
        withUsername("tiny_es")
        withPassword("tiny_es")
    }

    fun initialize(properties: Properties) {
        postgresContainer.start()

        properties.setProperty("datasource.jdbc-url", postgresContainer.jdbcUrl)
        properties.setProperty("datasource.username", postgresContainer.username)
        properties.setProperty("datasource.password", postgresContainer.password)
        properties.setProperty("tiny-es.storage.schema", "event_sourcing_store")
    }
}