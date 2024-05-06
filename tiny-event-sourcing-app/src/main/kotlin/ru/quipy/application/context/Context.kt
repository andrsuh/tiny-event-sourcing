package ru.quipy.application.context

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.bson.UuidRepresentation
import ru.quipy.TinyEsLibConfig
import ru.quipy.application.config.TinyEsDependencyConfig
import ru.quipy.bankDemo.BankContext
import ru.quipy.config.LiquibaseConfig
import ru.quipy.core.EventSourcingProperties
import ru.quipy.projectDemo.ProjectContext
import java.util.Properties

class Context private constructor() {
    lateinit var tinyEsLibConfig: TinyEsLibConfig

    lateinit var dataSource: HikariDataSource
    lateinit var mongoDatabase: MongoDatabase

    lateinit var projectContext: ProjectContext
    lateinit var bankContext: BankContext
    constructor(properties: Properties, eventSourcingProperties: EventSourcingProperties) : this() {
        this.dataSource = dataSource(properties)
        LiquibaseConfig().liquibase(dataSource, properties.getProperty("tiny-es.storage.schema"))

        this.mongoDatabase = mongoDatabase(properties)

        tinyEsLibConfig = TinyEsDependencyConfig(properties, dataSource, eventSourcingProperties)
            .tinyEsLibConfig

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
        val clientSettings = MongoClientSettings.builder()
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .applyConnectionString(ConnectionString(properties.getProperty("mongodb.url")))
            .build()

        return MongoClients.create(clientSettings)
            .getDatabase("tiny-es")
    }
}