package ru.quipy.autoconfigure

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import ru.quipy.PostgresClientEventStore
import ru.quipy.PostgresTemplateEventStore
import ru.quipy.config.DatabaseConfig
import ru.quipy.config.LiquibaseSpringConfig
import ru.quipy.converter.EntityConverter
import ru.quipy.converter.JsonEntityConverter
import ru.quipy.converter.ResultSetToEntityMapper
import ru.quipy.converter.ResultSetToEntityMapperImpl
import ru.quipy.core.EventSourcingProperties
import ru.quipy.db.HikariDatasourceProvider
import ru.quipy.db.factory.ConnectionFactory
import ru.quipy.db.factory.DataSourceConnectionFactoryImpl
import ru.quipy.executor.ExceptionLoggingSqlQueriesExecutor
import ru.quipy.executor.QueryExecutor
import ru.quipy.mappers.MapperFactory
import ru.quipy.mappers.MapperFactoryImpl
import javax.sql.DataSource

@Configuration
@Import(
    DatabaseConfig::class,
    LiquibaseSpringConfig::class
)
class PostgresEventStoreAutoConfiguration {
    @Value("\${event.sourcing.db-schema:event_sourcing_store}")
    private lateinit var schema: String

    @Bean
    @ConditionalOnMissingBean(ObjectMapper::class)
    fun objectMapper() : ObjectMapper {
        return jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
    }

    @Bean
    @ConditionalOnBean(ObjectMapper::class)
    fun entityConverter(
       objectMapper: ObjectMapper
    ) : EntityConverter {
        return JsonEntityConverter(objectMapper)
    }

    @Bean
    @ConditionalOnBean(EntityConverter::class)
    fun resultSetToEntityMapper(
       entityConverter: EntityConverter
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
    fun hikariDataSourceProvider(dataSource: HikariDataSource) : HikariDatasourceProvider {
        return HikariDatasourceProvider(dataSource)
    }

    @Bean
    @ConditionalOnBean(HikariDatasourceProvider::class)
    fun connectionFactory(hikariDataSourceProvider: HikariDatasourceProvider) : DataSourceConnectionFactoryImpl {
        return DataSourceConnectionFactoryImpl(hikariDataSourceProvider)
    }

    @Bean("exceptionLoggingSqlQueriesExecutor")
    @ConditionalOnBean(ConnectionFactory::class)
    fun executor(
        databaseFactory: ConnectionFactory
    ) : QueryExecutor {
        return ExceptionLoggingSqlQueriesExecutor(databaseFactory, PostgresClientEventStore.logger)
    }

    @Bean("postgresClientEventStore")
    @ConditionalOnBean(QueryExecutor::class, ResultSetToEntityMapper::class)
    fun postgresClientEventStore(
        resultSetToEntityMapper: ResultSetToEntityMapper,
        @Qualifier("exceptionLoggingSqlQueriesExecutor") executor: QueryExecutor,
        entityConverter: EntityConverter
    ) : PostgresClientEventStore {
        return PostgresClientEventStore(schema, resultSetToEntityMapper, entityConverter, executor)
    }

    @Primary
    @Bean("postgresTemplateEventStore")
    @ConditionalOnBean(JdbcTemplate::class, MapperFactory::class, EntityConverter::class)
    fun postgresTemplateEventStore(
        jdbcTemplate: JdbcTemplate,
        mapperFactory: MapperFactory,
        entityConverter: EntityConverter,
        props: EventSourcingProperties,
    ): PostgresTemplateEventStore {
        return PostgresTemplateEventStore(jdbcTemplate, schema, mapperFactory, entityConverter, props)
    }
}