package dev.ingest.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies that PostgreSQL (with Flyway baseline schema) and Kafka start and wire cleanly.
 */
@SpringBootTest
@Testcontainers
class InfrastructureSmokeIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void shouldCreateFlywayBaselineTablesAndPublishToKafka() throws Exception {
        List<String> tables = jdbcTemplate.queryForList(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """,
                String.class);

        assertThat(tables).contains(
                "extract_checkpoint",
                "consumer_offsets",
                "stream_schema",
                "schema_events",
                "dead_letters",
                "sync_runs");

        var sendResult = kafkaTemplate.send("ingest.smoke.test", "k1", "{\"hello\":\"world\"}").get();
        assertThat(sendResult.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0L);
    }
}
