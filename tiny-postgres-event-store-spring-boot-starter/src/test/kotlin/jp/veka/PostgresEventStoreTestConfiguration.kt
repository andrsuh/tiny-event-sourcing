package jp.veka

import jp.veka.autoconfigure.PostgresEventStoreAutoconfiguration
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles

@Configuration
@ActiveProfiles("test")
@Import(PostgresEventStoreAutoconfiguration::class)
class PostgresEventStoreTestConfiguration {
}