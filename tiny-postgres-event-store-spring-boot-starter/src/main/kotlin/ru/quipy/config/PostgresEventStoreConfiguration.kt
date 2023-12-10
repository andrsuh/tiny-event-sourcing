package ru.quipy.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import ru.quipy.PostgresClientEventStore
import ru.quipy.PostgresTemplateEventStore
import ru.quipy.converter.EntityConverter
import ru.quipy.converter.JsonEntityConverter
import ru.quipy.converter.ResultSetToEntityMapper
import ru.quipy.converter.ResultSetToEntityMapperImpl
import ru.quipy.db.DataSourceProvider
import ru.quipy.db.factory.ConnectionFactory
import ru.quipy.db.factory.ConnectionFactoryImpl
import ru.quipy.executor.ExceptionLoggingSqlQueriesExecutor
import ru.quipy.executor.QueryExecutor
import ru.quipy.mappers.MapperFactory
import ru.quipy.mappers.MapperFactoryImpl
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

    @Bean("jacksonObjectMapper")
    fun objectMapper() : ObjectMapper {
        return jacksonObjectMapper()
    }
    @Bean("jsonEntityConverter")
    fun entityConverter(
        @Qualifier("jacksonObjectMapper") objectMapper: ObjectMapper
    ) : EntityConverter {
        return JsonEntityConverter(objectMapper)
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
        @Value("\${batchInsertSize:1000}") batchInsertSize: Int
    ) : QueryExecutor {
        return ExceptionLoggingSqlQueriesExecutor(databaseFactory, batchInsertSize, PostgresClientEventStore.logger)
    }
    @Bean("postgresClientEventStore")
    fun postgresClientEventStore(
        @Qualifier("resultSetToEntityMapper") resultSetToEntityMapper: ResultSetToEntityMapper,
        @Qualifier("exceptionLoggingSqlQueriesExecutor") executor: QueryExecutor,
        entityConverter: EntityConverter
    ) : PostgresClientEventStore {
        return PostgresClientEventStore(schema, resultSetToEntityMapper,entityConverter, executor)
    }

    @Bean
    fun mapperFactory(resultSetToEntityMapper: ResultSetToEntityMapper) : MapperFactory {
        return MapperFactoryImpl(resultSetToEntityMapper)
    }
    @Bean("postgresTemplateEventStore")
    fun postgresTemplateEventStore(
        jdbcTemplate: JdbcTemplate,
        mapperFactory: MapperFactory,
        @Value("\${batchInsertSize:1000}") batchInsertSize: Int,
        entityConverter: EntityConverter
    ): ru.quipy.PostgresTemplateEventStore =
        ru.quipy.PostgresTemplateEventStore(jdbcTemplate, schema, mapperFactory, batchInsertSize, entityConverter)
}