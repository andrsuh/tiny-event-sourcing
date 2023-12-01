package jp.veka.mappers

import jp.veka.converter.ResultSetToEntityMapper
import org.springframework.jdbc.core.RowMapper
import ru.quipy.domain.EventStreamReadIndex
import java.sql.ResultSet

class EventStreamReadIndexRowMapper(private val entityMapper: ResultSetToEntityMapper) : RowMapper<EventStreamReadIndex> {
    override fun mapRow(rs: ResultSet, rowNum: Int): EventStreamReadIndex? {
        return entityMapper.convert(rs, EventStreamReadIndex::class)
    }
}