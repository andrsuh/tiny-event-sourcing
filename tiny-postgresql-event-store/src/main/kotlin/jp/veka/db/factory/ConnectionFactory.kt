package jp.veka.db.factory

import java.sql.Connection

interface ConnectionFactory {
    fun getDatabaseConnection() : Connection
}