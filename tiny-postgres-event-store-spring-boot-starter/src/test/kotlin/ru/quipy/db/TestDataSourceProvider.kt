package ru.quipy.db

import org.flywaydb.core.Flyway
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import javax.sql.DataSource

class TestDataSourceProvider : DataSourceProvider {
    private var dataSource: DataSource
    constructor(dbName: String, username: String, password: String, defaultSchema: String) {
        val container = PostgreSQLContainer(DockerImageName.parse("postgres:14.9-alpine")).apply {
            withDatabaseName(dbName)
            withUsername(username)
            withPassword(password)
        }
        if (!container.isRunning) {
            container.start()
        }
        dataSource = PGSimpleDataSource().apply {
            setURL(container.jdbcUrl)
            user = username
            this.password = password
        }
        val flyway = Flyway.configure()
            .locations("/migrations")
            .dataSource(dataSource)
            .defaultSchema(defaultSchema)
            .load()

        flyway.migrate()
    }
    override fun dataSource(): DataSource {
        return dataSource
    }
}