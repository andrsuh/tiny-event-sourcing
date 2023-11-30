package jp.veka.db.factory

import jp.veka.db.DataSourceProvider
import java.sql.Connection

class ConnectionFactoryImpl(private val dataSourceProvider: DataSourceProvider) : ConnectionFactory {
    override fun getDatabaseConnection(): Connection {
        return this.dataSourceProvider.dataSource().connection
    }
}