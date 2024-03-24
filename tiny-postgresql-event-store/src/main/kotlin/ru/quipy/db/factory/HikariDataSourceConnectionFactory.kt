package ru.quipy.db.factory

import ru.quipy.db.HikariDatasourceProvider
import java.sql.Connection

class HikariDataSourceConnectionFactory(private val dataSourceProvider: HikariDatasourceProvider) : ConnectionFactory {
    override fun getDatabaseConnection(): Connection {
        return this.dataSourceProvider.dataSource().connection
    }
}