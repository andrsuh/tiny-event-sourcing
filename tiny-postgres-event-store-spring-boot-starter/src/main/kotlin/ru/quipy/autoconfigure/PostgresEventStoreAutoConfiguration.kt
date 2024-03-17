package ru.quipy.autoconfigure

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import ru.quipy.PostgresClientEventStore
import ru.quipy.PostgresTemplateEventStore
import ru.quipy.config.DatabaseConfig
import ru.quipy.config.LiquibaseConfig
import ru.quipy.converter.EntityConverter
import ru.quipy.converter.JsonEntityConverter
import ru.quipy.converter.ResultSetToEntityMapper
import ru.quipy.converter.ResultSetToEntityMapperImpl
import ru.quipy.db.HikariDatasourceProvider
import ru.quipy.db.factory.ConnectionFactory
import ru.quipy.db.factory.HikariDataSourceConnectionFactory
import ru.quipy.executor.ExceptionLoggingSqlQueriesExecutor
import ru.quipy.executor.QueryExecutor
import ru.quipy.mappers.MapperFactory
import ru.quipy.mappers.MapperFactoryImpl
import javax.sql.DataSource

@Configuration
@Import(
    DatabaseConfig::class,
    LiquibaseConfig::class
)
class PostgresEventStoreAutoConfiguration {
    @Value("\${schema:event_sourcing_store}")
    private lateinit var schema: String

    @Bean
    fun objectMapper() : ObjectMapper {
        return jacksonObjectMapper()
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
    fun connectionFactory(hikariDataSourceProvider: HikariDatasourceProvider) : HikariDataSourceConnectionFactory {
        return HikariDataSourceConnectionFactory(hikariDataSourceProvider)
    }

    @Bean("exceptionLoggingSqlQueriesExecutor")
    @ConditionalOnBean(ConnectionFactory::class)
    fun executor(
        databaseFactory: ConnectionFactory,
        @Value("\${batchInsertSize:1000}") batchInsertSize: Int
    ) : QueryExecutor {
        return ExceptionLoggingSqlQueriesExecutor(databaseFactory, batchInsertSize, PostgresClientEventStore.logger)
    }

    // @Primary
    @Bean("postgresClientEventStore")
    @ConditionalOnBean(QueryExecutor::class, ResultSetToEntityMapper::class)
    fun postgresClientEventStore(
        resultSetToEntityMapper: ResultSetToEntityMapper,
        @Qualifier("exceptionLoggingSqlQueriesExecutor") executor: QueryExecutor,
        entityConverter: EntityConverter
    ) : PostgresClientEventStore {
        return PostgresClientEventStore(schema, resultSetToEntityMapper, entityConverter, executor)
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
    ): PostgresTemplateEventStore {
        return PostgresTemplateEventStore(jdbcTemplate, schema, mapperFactory, batchInsertSize, entityConverter)
    }
}