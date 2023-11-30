package jp.veka.config

import jp.veka.PostgresClientEventStore
import jp.veka.PostgresTemplateEventStore
import jp.veka.converter.EntityConverter
import jp.veka.converter.JsonEntityConverter
import jp.veka.converter.ResultSetToEntityMapper
import jp.veka.converter.ResultSetToEntityMapperImpl
import jp.veka.db.DataSourceProvider
import jp.veka.db.factory.ConnectionFactory
import jp.veka.db.factory.ConnectionFactoryImpl
import jp.veka.executor.ExceptionLoggingSqlQueriesExecutor
import jp.veka.executor.Executor
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class PostgresEventStoreConfiguration {
    @Bean
    fun databaseFactory(datasourceProvider: DataSourceProvider) : ConnectionFactory {
        return ConnectionFactoryImpl(datasourceProvider)
    }

    @Bean("jsonEntityConverter")
    fun entityConverter() : EntityConverter {
        return JsonEntityConverter()
    }
    @Bean("resultSetToEntityMapper")
    fun resultSetToEntityMapper(
        @Qualifier("jsonEntityConverter") entityConverter: EntityConverter
    ) : ResultSetToEntityMapper {
        return ResultSetToEntityMapperImpl(entityConverter)
    }

    @Bean("exceptionLoggingSqlQueriesExecutor")
    fun executor() : Executor {
        return ExceptionLoggingSqlQueriesExecutor(PostgresClientEventStore.logger)
    }
    @Bean("postgresClientEventStore")
    fun postgresClientEventStore(
        databaseFactory: ConnectionFactory,
        @Value("\${schema:event_sourcing_store}") schema: String,
        @Qualifier("resultSetToEntityMapper") resultSetToEntityMapper: ResultSetToEntityMapper,
        @Qualifier("exceptionLoggingSqlQueriesExecutor") executor: Executor
    ) : PostgresClientEventStore {
        return PostgresClientEventStore(databaseFactory, schema, resultSetToEntityMapper, executor)
    }

    @Bean("postgresTemplateEventStore")
    fun postgresTemplateEventStore(): PostgresTemplateEventStore = PostgresTemplateEventStore()
}