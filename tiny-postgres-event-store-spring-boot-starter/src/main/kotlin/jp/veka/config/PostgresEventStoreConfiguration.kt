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
import jp.veka.executor.QueryExecutor
import jp.veka.mappers.MapperFactory
import jp.veka.mappers.MapperFactoryImpl
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate

@Configuration
class PostgresEventStoreConfiguration {
    @Value("\${schema:event_sourcing_store}")
    private lateinit var schema: String
    @Bean
    fun connectionFactory(datasourceProvider: DataSourceProvider) : ConnectionFactory {
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
    fun executor(
        databaseFactory: ConnectionFactory,
        @Value("\${batchInsertSize:1000}") batchInsertSize: Long
    ) : QueryExecutor {
        return ExceptionLoggingSqlQueriesExecutor(databaseFactory, batchInsertSize, PostgresClientEventStore.logger)
    }
    @Bean("postgresClientEventStore")
    fun postgresClientEventStore(
        @Qualifier("resultSetToEntityMapper") resultSetToEntityMapper: ResultSetToEntityMapper,
        @Qualifier("exceptionLoggingSqlQueriesExecutor") executor: QueryExecutor
    ) : PostgresClientEventStore {
        return PostgresClientEventStore(schema, resultSetToEntityMapper, executor)
    }

    @Bean
    fun mapperFactory(resultSetToEntityMapper: ResultSetToEntityMapper) : MapperFactory {
        return MapperFactoryImpl(resultSetToEntityMapper)
    }
    @Bean("postgresTemplateEventStore")
    fun postgresTemplateEventStore(
        jdbcTemplate: JdbcTemplate,
        mapperFactory: MapperFactory
    ): PostgresTemplateEventStore = PostgresTemplateEventStore(jdbcTemplate, schema, mapperFactory)
}