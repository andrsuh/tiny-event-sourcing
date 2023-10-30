package jp.veka.factory

import java.sql.Connection

interface PostgresConnectionFactory {
    fun getDatabaseConnection() : Connection;

}