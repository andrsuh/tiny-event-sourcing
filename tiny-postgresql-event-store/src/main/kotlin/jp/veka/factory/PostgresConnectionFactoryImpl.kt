package jp.veka.factory

import java.sql.Connection
import java.sql.DriverManager

class PostgresConnectionFactoryImpl(databaseUrl: String, user: String, password: String) : PostgresConnectionFactory {
    private val connection: Connection

    init {
        this.connection = DriverManager.getConnection(databaseUrl, user, password)
    }
    override fun getDatabaseConnection(): Connection {
        return this.connection
    }
}