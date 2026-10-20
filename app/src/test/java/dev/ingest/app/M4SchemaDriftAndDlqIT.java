package dev.ingest.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.ingest.core.SourceRecord;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * M4 Acceptance Test: Verifies online column addition, safe type widening (INTEGER -> BIGINT -> NUMERIC),
 * NULL handling for missing fields, and DLQ routing for incompatible types and malformed JSON payloads.
 */
@SpringBootTest(classes = IngestPipelineApplication.class)
@Testcontainers
class M4SchemaDriftAndDlqIT {

    private static final String TOPIC = "ingest.raw.rdw_vehicles";

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withUrlParam("reWriteBatchedInserts", "true");

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void handlesColumnAdditionTypeWideningMissingFieldsAndRoutesPoisonRecordsToDlq() throws Exception {
        jdbcTemplate.execute("DELETE FROM schema_events WHERE stream = 'rdw_vehicles'");
        jdbcTemplate.execute("DELETE FROM dead_letters WHERE stream = 'rdw_vehicles'");

        // 1. Record with new columns: aantal_deuren (INTEGER) and is_elektrisch (BOOLEAN)
        Map<String, Object> p1 = new LinkedHashMap<>();
        p1.put("kenteken", "DRIFT01");
        p1.put("merk", "VOLVO");
        p1.put("massa_ledig_voertuig", 1500);
        p1.put("aantal_deuren", 5);
        p1.put("is_elektrisch", true);
        publish(SourceRecord.create("rdw_vehicles", "DRIFT01", p1, Instant.now()));

        // 2. Record widening massa_ledig_voertuig from INTEGER -> BIGINT (> Integer.MAX_VALUE)
        Map<String, Object> p2 = new LinkedHashMap<>();
        p2.put("kenteken", "DRIFT02");
        p2.put("merk", "DAF");
        p2.put("massa_ledig_voertuig", 5_000_000_000L);
        publish(SourceRecord.create("rdw_vehicles", "DRIFT02", p2, Instant.now()));

        // 3. Record widening massa_ledig_voertuig from BIGINT -> NUMERIC (decimal weight) & missing 'merk'
        Map<String, Object> p3 = new LinkedHashMap<>();
        p3.put("kenteken", "DRIFT03");
        p3.put("massa_ledig_voertuig", 1845.75);
        publish(SourceRecord.create("rdw_vehicles", "DRIFT03", p3, Instant.now()));

        // 4. Incompatible schema type: massa_ledig_voertuig is TEXT ("NOT_A_NUMBER") -> must go to dead_letters
        Map<String, Object> p4BadType = new LinkedHashMap<>();
        p4BadType.put("kenteken", "DRIFT_BAD_TYPE");
        p4BadType.put("merk", "BMW");
        p4BadType.put("massa_ledig_voertuig", "NOT_A_NUMBER");
        publish(SourceRecord.create("rdw_vehicles", "DRIFT_BAD_TYPE", p4BadType, Instant.now()));

        // 5. Malformed raw JSON record -> must go to dead_letters without crashing the batch
        kafkaTemplate.send(TOPIC, "DRIFT_CORRUPT_JSON", "{corrupt-unparseable-json").get();

        // Wait until both dead_letters and valid drift records are persisted
        awaitCondition(() -> {
            Long deadCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM dead_letters WHERE stream = 'rdw_vehicles'", Long.class);
            Long validDriftCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM rdw_vehicles WHERE pk IN ('DRIFT01', 'DRIFT02', 'DRIFT03')", Long.class);
            return deadCount != null && deadCount >= 2 && validDriftCount != null && validDriftCount == 3;
        }, Duration.ofSeconds(30));

        // Verify newly added columns and widened numeric values in rdw_vehicles
        Integer doors = jdbcTemplate.queryForObject(
                "SELECT aantal_deuren FROM rdw_vehicles WHERE pk = 'DRIFT01'", Integer.class);
        Boolean electric = jdbcTemplate.queryForObject(
                "SELECT is_elektrisch FROM rdw_vehicles WHERE pk = 'DRIFT01'", Boolean.class);
        BigDecimal bigWeight = jdbcTemplate.queryForObject(
                "SELECT massa_ledig_voertuig FROM rdw_vehicles WHERE pk = 'DRIFT02'", BigDecimal.class);
        BigDecimal decimalWeight = jdbcTemplate.queryForObject(
                "SELECT massa_ledig_voertuig FROM rdw_vehicles WHERE pk = 'DRIFT03'", BigDecimal.class);
        String missingMerk = jdbcTemplate.queryForObject(
                "SELECT merk FROM rdw_vehicles WHERE pk = 'DRIFT03'", String.class);

        assertThat(doors).isEqualTo(5);
        assertThat(electric).isTrue();
        assertThat(bigWeight).isEqualByComparingTo(new BigDecimal("5000000000"));
        assertThat(decimalWeight).isEqualByComparingTo(new BigDecimal("1845.75"));
        assertThat(missingMerk).isNull();

        // Verify schema_events recorded 2 COLUMN_ADDED and 2 COLUMN_WIDENED events
        List<String> eventTypes = jdbcTemplate.queryForList(
                "SELECT event_type || ':' || column_name FROM schema_events WHERE stream = 'rdw_vehicles' ORDER BY id",
                String.class);
        assertThat(eventTypes).contains(
                "COLUMN_ADDED:aantal_deuren",
                "COLUMN_ADDED:is_elektrisch",
                "COLUMN_WIDENED:massa_ledig_voertuig");

        // Verify dead_letters contains both INCOMPATIBLE_SCHEMA_TYPE and MALFORMED_RECORD_JSON
        List<String> deadReasons = jdbcTemplate.queryForList(
                "SELECT reason_code FROM dead_letters WHERE stream = 'rdw_vehicles'",
                String.class);
        assertThat(deadReasons).contains("INCOMPATIBLE_SCHEMA_TYPE", "MALFORMED_RECORD_JSON");
    }

    private void publish(SourceRecord record) throws Exception {
        kafkaTemplate.send(TOPIC, record.primaryKey(), objectMapper.writeValueAsString(record)).get();
    }

    private void awaitCondition(java.util.function.BooleanSupplier condition, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (Exception ignored) {
                // Table may not exist on first poll
            }
            Thread.sleep(150L);
        }
        throw new AssertionError("Timed out waiting for M4 schema drift and DLQ assertions");
    }
}
