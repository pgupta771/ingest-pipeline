package dev.ingest.app;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ingest.app.fixture.RdwFixtureServer;
import dev.ingest.connector.rdw.RdwConnectorConfig;
import dev.ingest.connector.rdw.RdwVehicleConnector;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.engine.ExtractionOrchestrator;
import dev.ingest.engine.SyncMode;
import dev.ingest.engine.SyncRunInfo;
import java.sql.Timestamp;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * M2 Acceptance Test: Extracts and loads the 200,000-record deterministic fixture twice
 * and verifies identical row counts, identical content hashes, and zero duplicate writes on replay.
 */
@SpringBootTest(classes = IngestPipelineApplication.class)
@Testcontainers
class M2TransportAndLoaderIT {

    private static final int TOTAL_FIXTURE_RECORDS = 200_000;
    private static RdwFixtureServer fixtureServer;

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
    private ConnectorRegistry connectorRegistry;

    @Autowired
    private ExtractionOrchestrator orchestrator;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void startFixture() throws Exception {
        fixtureServer = new RdwFixtureServer(TOTAL_FIXTURE_RECORDS);
    }

    @AfterAll
    static void stopFixture() {
        if (fixtureServer != null) {
            fixtureServer.close();
        }
    }

    @Test
    void loading200kFixtureTwiceYieldsIdenticalRowCountsAndHashes() throws Exception {
        connectorRegistry.register(
                new RdwVehicleConnector(RdwConnectorConfig.defaults(fixtureServer.baseUri())));

        // 1st Full Load of 200,000 records
        SyncRunInfo firstRun = orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.FULL, 5000);
        assertThat(firstRun.recordsExtracted()).isEqualTo(TOTAL_FIXTURE_RECORDS);

        awaitRowCount("rdw_vehicles", TOTAL_FIXTURE_RECORDS, Duration.ofSeconds(60));

        long countAfterFirst = queryRowCount("rdw_vehicles");
        String checksumAfterFirst = queryDatasetChecksum("rdw_vehicles");
        Timestamp maxLoadedAtAfterFirst = jdbcTemplate.queryForObject(
                "SELECT MAX(loaded_at) FROM rdw_vehicles", Timestamp.class);

        // 2nd Full Load of the exact same 200,000 records (400,000 total Kafka messages published)
        SyncRunInfo secondRun = orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.FULL, 5000);
        assertThat(secondRun.recordsExtracted()).isEqualTo(TOTAL_FIXTURE_RECORDS);

        awaitTotalOffset(2L * TOTAL_FIXTURE_RECORDS, Duration.ofSeconds(60));

        long countAfterSecond = queryRowCount("rdw_vehicles");
        String checksumAfterSecond = queryDatasetChecksum("rdw_vehicles");
        Timestamp maxLoadedAtAfterSecond = jdbcTemplate.queryForObject(
                "SELECT MAX(loaded_at) FROM rdw_vehicles", Timestamp.class);

        assertThat(countAfterFirst).isEqualTo(TOTAL_FIXTURE_RECORDS);
        assertThat(countAfterSecond).isEqualTo(TOTAL_FIXTURE_RECORDS);
        assertThat(checksumAfterSecond).isEqualTo(checksumAfterFirst).isNotBlank();
        // Because source_hash is identical, ON CONFLICT ... WHERE source_hash IS DISTINCT performed 0 row updates
        assertThat(maxLoadedAtAfterSecond).isEqualTo(maxLoadedAtAfterFirst);
    }

    private void awaitRowCount(String table, long expected, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (tableExists(table) && queryRowCount(table) >= expected) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Timed out waiting for " + expected + " rows in " + table);
    }

    private void awaitTotalOffset(long expectedOffsetSum, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Long sum = jdbcTemplate.queryForObject(
                    "SELECT COALESCE(SUM(next_offset), 0) FROM consumer_offsets WHERE topic = 'ingest.raw.rdw_vehicles'",
                    Long.class);
            if (sum != null && sum >= expectedOffsetSum) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Timed out waiting for consumer_offsets to reach " + expectedOffsetSum);
    }

    private boolean tableExists(String table) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
                Integer.class,
                table);
        return count != null && count > 0;
    }

    private long queryRowCount(String table) {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
        return count != null ? count : 0L;
    }

    private String queryDatasetChecksum(String table) {
        return jdbcTemplate.queryForObject(
                "SELECT md5(string_agg(pk || ':' || source_hash, ',' ORDER BY pk)) FROM " + table,
                String.class);
    }
}
