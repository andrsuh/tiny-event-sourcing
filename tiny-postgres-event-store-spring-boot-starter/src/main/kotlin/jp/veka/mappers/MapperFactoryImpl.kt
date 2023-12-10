package jp.veka.mappers

import jp.veka.converter.ResultSetToEntityMapper
import org.springframework.jdbc.core.RowMapper
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.domain.Snapshot
import kotlin.reflect.KClass

class MapperFactoryImpl(private val entityMapper: ResultSetToEntityMapper) : MapperFactory {
    override fun <T : Any> getMapper(clazz: KClass<T>): RowMapper<T>  {
        return when (clazz) {
            EventRecord::class -> EventRowMapper(entityMapper)
            Snapshot::class -> SnapshotRowMapper(entityMapper)
            EventStreamReadIndex::class -> EventStreamReadIndexRowMapper(entityMapper)
            ActiveEventStreamReader::class -> ActiveEventStreamReaderRowMapper(entityMapper)
            else -> throw IllegalStateException("No mapper defined for entity ${clazz.simpleName}")
        }  as RowMapper<T>
    }
}