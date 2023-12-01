package jp.veka.mappers

import jp.veka.converter.ResultSetToEntityMapper
import org.springframework.jdbc.core.RowMapper
import ru.quipy.domain.EventRecord
import java.sql.ResultSet

class EventRowMapper(private val entityMapper: ResultSetToEntityMapper) : RowMapper<EventRecord> {
    override fun mapRow(rs: ResultSet, rowNum: Int): EventRecord? {
        return entityMapper.convert(rs, EventRecord::class, false)
    }
}