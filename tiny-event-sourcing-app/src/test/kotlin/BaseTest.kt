package ru.quipy

import com.mongodb.client.MongoDatabase
import ru.quipy.application.context.Context
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.db.BankAccountCacheRepository
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.bankDemo.transfers.service.TransactionService
import ru.quipy.config.DockerPostgresDataSourceInitializer
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.database.EventStore
import ru.quipy.db.DatasourceProviderImpl
import ru.quipy.db.factory.DataSourceConnectionFactoryImpl
import ru.quipy.mapper.EventMapper
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.logic.ProjectAggregateState
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.EventStreamReaderManager
import ru.quipy.tables.EventRecordTable
import ru.quipy.tables.EventStreamActiveReadersTable
import ru.quipy.tables.EventStreamReadIndexTable
import ru.quipy.tables.SnapshotTable
import java.util.Properties
import java.util.UUID

open class BaseTest(private val testId: String) {
    companion object {
        lateinit var mongoDatabase: MongoDatabase

        lateinit var schema: String
        lateinit var databaseConnectionFactory: ru.quipy.db.factory.ConnectionFactory
        lateinit var eventStreamManager: AggregateEventStreamManager //= AggregateEventStreamManager(registry, eventStore, properties)

        lateinit var eventStreamReaderManager: EventStreamReaderManager //= AggregateEventStreamManager(registry, eventStore, properties)

        lateinit var subscriptionsManager: AggregateSubscriptionsManager// = AggregateSubscriptionsManager(eventStreamManager, registry, eventMapper)

        lateinit var demoESService: EventSourcingService<String, ProjectAggregate, ProjectAggregateState>
        lateinit var registry: AggregateRegistry
        lateinit var eventStore: EventStore
        lateinit var eventMapper: EventMapper

        lateinit var accountEsService: EventSourcingService<UUID, AccountAggregate, Account>
        lateinit var transactionEsService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>
        lateinit var transactionService: TransactionService
        lateinit var bankAccountCacheRepository: BankAccountCacheRepository
        lateinit var bankESService: EventSourcingService<UUID, AccountAggregate, Account>

        fun configure(eventSourcingProperties: EventSourcingProperties) {
            val properties = Properties()
            properties.setProperty("mongodb.url", "mongodb://localhost:27017")
            DockerPostgresDataSourceInitializer().initialize(properties) // run docker db and init properties
            val context = Context(properties, EventSourcingProperties(
                scanPackage = "ru.quipy",
                autoScanEnabled = true))
            val tinyEsLibConfig = context.tinyEsLibConfig

            mongoDatabase = context.mongoDatabase
            schema = properties.getProperty("event.sourcing.db-schema")
            databaseConnectionFactory = DataSourceConnectionFactoryImpl(DatasourceProviderImpl(context.dataSource))
            eventStreamManager = tinyEsLibConfig.eventStreamManager
            eventStreamReaderManager = tinyEsLibConfig.eventStreamReaderManager
            subscriptionsManager = tinyEsLibConfig.subscriptionsManager
            demoESService = context.projectContext.projectDemoConfig.demoESService()
            registry = tinyEsLibConfig.aggregateRegistry
            eventStore = tinyEsLibConfig.eventStore
            eventMapper = tinyEsLibConfig.eventMapper

            accountEsService = context.bankContext.accountEventSourcingService
            transactionEsService = context.bankContext.transactionEventSourcingService
            transactionService = context.bankContext.transactionService
            bankAccountCacheRepository = context.bankContext.bankAccountCacheRepository
            bankESService = context.bankContext.accountEventSourcingService
        }
    }
    open fun cleanDatabase() {
        mongoDatabase.getCollection("aggregate-project").drop()
        mongoDatabase.getCollection("snapshots").drop()

        databaseConnectionFactory.getDatabaseConnection().use { connection ->  connection.createStatement().execute(
                "truncate ${schema}.${EventRecordTable.name};" +
                    "truncate ${schema}.${SnapshotTable.name};" +
                    "truncate ${schema}.${EventStreamReadIndexTable.name};" +
                    "truncate ${schema}.${EventStreamActiveReadersTable.name};")
        }
    }
}