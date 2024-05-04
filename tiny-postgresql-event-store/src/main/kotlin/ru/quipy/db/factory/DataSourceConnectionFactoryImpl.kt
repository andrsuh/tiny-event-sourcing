package ru.quipy.db.factory

import ru.quipy.db.DataSourceProvider
import java.sql.Connection

class DataSourceConnectionFactoryImpl(private val dataSourceProvider: DataSourceProvider) : ConnectionFactory {
    override fun getDatabaseConnection(): Connection {
        return this.dataSourceProvider.dataSource().connection
    }
}