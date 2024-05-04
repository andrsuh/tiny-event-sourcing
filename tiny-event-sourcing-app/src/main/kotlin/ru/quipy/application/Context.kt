package ru.quipy.application

import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ru.quipy.TinyEsLibConfig
import ru.quipy.bankDemo.BankContext
import ru.quipy.projectDemo.ProjectContext
import java.util.Properties

class Context private constructor() {
    private lateinit var tinyEsLibConfig: TinyEsLibConfig
    private lateinit var projectContext: ProjectContext
    private lateinit var dataSource: HikariDataSource
    private lateinit var mongoDatabase: MongoDatabase

    private lateinit var bankContext: BankContext
    constructor(properties: Properties) : this() {
        this.dataSource = dataSource(properties)
        mongoDatabase = mongoDatabase(properties)
        tinyEsLibConfig = TinyEsLibConfig(properties, dataSource)

        projectContext = ProjectContext(tinyEsLibConfig)
        bankContext = BankContext(tinyEsLibConfig, mongoDatabase)
    }


    private fun dataSource(properties: Properties) : HikariDataSource {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = properties.getProperty("datasource.jdbc-url")
        hikariConfig.username = properties.getProperty("datasource.username")
        hikariConfig.password = properties.getProperty("datasource.password")

        return HikariDataSource(hikariConfig)
    }

    private fun mongoDatabase(properties: Properties) : MongoDatabase {
        return MongoClients.create(properties.getProperty("mongodb.url")).getDatabase("tiny-es")
    }
}