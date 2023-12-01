package jp.veka.mappers

import jp.veka.converter.ResultSetToEntityMapper
import org.springframework.jdbc.core.RowMapper
import ru.quipy.domain.ActiveEventStreamReader
import java.sql.ResultSet

class ActiveEventStreamReaderRowMapper(private val entityMapper: ResultSetToEntityMapper) : RowMapper<ActiveEventStreamReader> {
    override fun mapRow(rs: ResultSet, rowNum: Int): ActiveEventStreamReader? {
        return entityMapper.convert(rs, ActiveEventStreamReader::class)
    }
}