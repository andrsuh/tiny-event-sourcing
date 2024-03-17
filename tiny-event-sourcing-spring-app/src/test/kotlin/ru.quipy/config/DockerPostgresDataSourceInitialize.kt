package ru.quipy.config

import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.support.TestPropertySourceUtils
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class DockerPostgresDataSourceInitializer : ApplicationContextInitializer<ConfigurableApplicationContext?> {
    private val postgresContainer = PostgreSQLContainer(DockerImageName.parse("postgres:14.9-alpine")).apply {
        withDatabaseName("tiny_es")
        withUsername("tiny_es")
        withPassword("tiny_es")
    }

    override fun initialize(configurableApplicationContext: ConfigurableApplicationContext) {
        postgresContainer.start()

        TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
            configurableApplicationContext,
            "jdbc.connectionString=" + postgresContainer.jdbcUrl,
            "jdbc.username=" + postgresContainer.username,
            "jdbc.password=" + postgresContainer.password
        )
    }

}