package ru.quipy.mappers

import ru.quipy.converter.ResultSetToEntityMapper
import org.springframework.jdbc.core.RowMapper
import ru.quipy.domain.Snapshot
import java.sql.ResultSet

class SnapshotRowMapper(private val entityMapper: ResultSetToEntityMapper) : RowMapper<Snapshot> {
    override fun mapRow(rs: ResultSet, rowNum: Int): Snapshot? {
        return entityMapper.convert(rs, Snapshot::class, false)
    }
}