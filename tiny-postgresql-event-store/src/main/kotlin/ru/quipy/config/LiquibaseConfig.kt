package ru.quipy.config

import liquibase.Liquibase
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.FileSystemResourceAccessor
import org.springframework.core.io.FileSystemResourceLoader
import java.io.File
import java.sql.SQLException
import javax.sql.DataSource

class LiquibaseConfig {
    fun liquibase(dataSource: DataSource, schema: String): Liquibase {
        try {
            dataSource.connection.use { connection ->
                connection.createStatement()
                    .execute("CREATE SCHEMA IF NOT EXISTS $schema;")
            }
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
        dataSource.connection.use { connection ->
            val databaseConnection = JdbcConnection(connection)
            val chageset = LiquibaseConfig::class.java.classLoader.getResource("liquibase").path
            val fileSystemResourceAccessor = FileSystemResourceAccessor(File(chageset))
            val liquibase = Liquibase("changelog.sql", fileSystemResourceAccessor, databaseConnection)
            liquibase.setChangeLogParameter("schema", schema)
            liquibase.update("")
            return liquibase
        }
    }

    fun getResourceReader() : FileSystemResourceLoader {
        return FileSystemResourceLoader()
    }
}