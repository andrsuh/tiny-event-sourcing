package ru.quipy.autoconfigure

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import ru.quipy.PostgresClientEventStore
import ru.quipy.converter.EntityConverter
import ru.quipy.converter.JsonEntityConverter
import ru.quipy.converter.ResultSetToEntityMapper
import ru.quipy.converter.ResultSetToEntityMapperImpl
import ru.quipy.db.DataSourceProvider
import ru.quipy.db.DatasourceProviderImpl
import ru.quipy.db.factory.ConnectionFactory
import ru.quipy.db.factory.ConnectionFactoryImpl
import ru.quipy.executor.ExceptionLoggingSqlQueriesExecutor
import ru.quipy.executor.QueryExecutor
import ru.quipy.mappers.MapperFactory
import ru.quipy.mappers.MapperFactoryImpl
import javax.sql.DataSource

@Configuration
class PostgresEventStoreAutoConfiguration {
    @Value("\${schema:event_sourcing_store}")
    private lateinit var schema: String

    @Bean("jacksonObjectMapper")
    fun objectMapper() : ObjectMapper {
        return jacksonObjectMapper()
    }
    @Bean("jsonEntityConverter")
    @ConditionalOnBean(ObjectMapper::class)
    fun entityConverter(
        @Qualifier("jacksonObjectMapper") objectMapper: ObjectMapper
    ) : EntityConverter {
        return JsonEntityConverter(objectMapper)
    }
    @Bean("resultSetToEntityMapper")
    @ConditionalOnBean(EntityConverter::class)
    fun resultSetToEntityMapper(
        @Qualifier("jsonEntityConverter") entityConverter: EntityConverter
    ) : ResultSetToEntityMapper {
        return ResultSetToEntityMapperImpl(entityConverter)
    }

    @Bean
    @ConditionalOnBean(ResultSetToEntityMapper::class)
    fun mapperFactory(resultSetToEntityMapper: ResultSetToEntityMapper) : MapperFactory {
        return MapperFactoryImpl(resultSetToEntityMapper)
    }
    @Bean
    @ConditionalOnBean(DataSource::class)
    fun dataSourceProvider(dataSource: DataSource) : DataSourceProvider {
        return DatasourceProviderImpl(dataSource)
    }

    @Bean
    @ConditionalOnBean(DataSourceProvider::class)
    fun connectionFactory(dataSourceProvider: DataSourceProvider) : ConnectionFactory {
        return ConnectionFactoryImpl(dataSourceProvider)
    }

    @Bean("exceptionLoggingSqlQueriesExecutor")
    @ConditionalOnBean(ConnectionFactory::class)
    fun executor(
        databaseFactory: ConnectionFactory,
        @Value("\${batchInsertSize:1000}") batchInsertSize: Int
    ) : QueryExecutor {
        return ExceptionLoggingSqlQueriesExecutor(databaseFactory, batchInsertSize, PostgresClientEventStore.logger)
    }

    @Bean("postgresClientEventStore")
    @ConditionalOnBean(QueryExecutor::class, ResultSetToEntityMapper::class)
    fun postgresClientEventStore(
        @Qualifier("resultSetToEntityMapper") resultSetToEntityMapper: ResultSetToEntityMapper,
        @Qualifier("exceptionLoggingSqlQueriesExecutor") executor: QueryExecutor,
        entityConverter: EntityConverter
    ) : PostgresClientEventStore {
        return PostgresClientEventStore(schema, resultSetToEntityMapper,entityConverter, executor)
    }

    @Bean
    @ConditionalOnBean(DataSource::class)
    fun jdbcTemplate(dataSource: DataSource) : JdbcTemplate {
        return JdbcTemplate(dataSource)
    }

    @Primary
    @Bean("postgresTemplateEventStore")
    @ConditionalOnBean(JdbcTemplate::class, MapperFactory::class, EntityConverter::class)
    fun postgresTemplateEventStore(
        jdbcTemplate: JdbcTemplate,
        mapperFactory: MapperFactory,
        @Value("\${batchInsertSize:1000}") batchInsertSize: Int,
        entityConverter: EntityConverter
    ): ru.quipy.PostgresTemplateEventStore =
        ru.quipy.PostgresTemplateEventStore(jdbcTemplate, schema, mapperFactory, batchInsertSize, entityConverter)
}