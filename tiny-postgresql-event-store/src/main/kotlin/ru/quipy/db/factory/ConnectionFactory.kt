package ru.quipy.db.factory

import java.sql.Connection

interface ConnectionFactory {
    fun getDatabaseConnection() : Connection
}