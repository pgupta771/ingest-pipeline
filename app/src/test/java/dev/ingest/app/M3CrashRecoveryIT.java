package dev.ingest.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ingest.app.fixture.RdwFixtureServer;
import dev.ingest.connector.rdw.RdwConnectorConfig;
import dev.ingest.connector.rdw.RdwVehicleConnector;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.engine.ExtractionOrchestrator;
import dev.ingest.engine.FaultInjectionHook;
import dev.ingest.engine.SyncMode;
import dev.ingest.engine.SyncRunInfo;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * M3 Headline Acceptance Test: Proves zero duplicates and zero gaps across 10 consecutive
 * multi-crash-and-resume runs where crashes occur after Kafka publish but before checkpoint commit.
 */
@SpringBootTest(classes = IngestPipelineApplication.class)
@Testcontainers
class M3CrashRecoveryIT {

    private static final int FIXTURE_RECORDS = 10_000;
    private static final int PAGE_SIZE = 1_000;
    private static final int CONSECUTIVE_RUNS = 10;

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
        fixtureServer = new RdwFixtureServer(FIXTURE_RECORDS);
    }

    @AfterAll
    static void stopFixture() {
        if (fixtureServer != null) {
            fixtureServer.close();
        }
    }

    @Test
    void recoversFromMidSyncCrashesWithZeroDuplicatesAndZeroGapsAcross10ConsecutiveRuns() throws Exception {
        connectorRegistry.register(
                new RdwVehicleConnector(RdwConnectorConfig.defaults(fixtureServer.baseUri())));

        orchestrator.setFaultHook(FaultInjectionHook.NO_OP);
        orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.FULL, PAGE_SIZE);
        awaitRowCount("rdw_vehicles", FIXTURE_RECORDS, Duration.ofSeconds(30));
        String expectedChecksum = queryChecksum("rdw_vehicles");
        assertThat(expectedChecksum).isNotBlank();

        for (int run = 1; run <= CONSECUTIVE_RUNS; run++) {
            jdbcTemplate.execute("TRUNCATE TABLE rdw_vehicles");
            Set<Integer> firedFaultPoints = ConcurrentHashMap.newKeySet();
            int crashPoint1 = (run % 3) + 2;
            int crashPoint2 = crashPoint1 + 3;

            orchestrator.setFaultHook((stream, pageNumber, recordsPublished) -> {
                if (pageNumber == crashPoint1 && firedFaultPoints.add(1)) {
                    throw new RuntimeException("SIMULATED_SIGKILL_CRASH_1_BEFORE_CHECKPOINT");
                }
                if (pageNumber == crashPoint2 && firedFaultPoints.add(2)) {
                    throw new RuntimeException("SIMULATED_SIGKILL_CRASH_2_BEFORE_CHECKPOINT");
                }
            });

            assertThatThrownBy(() -> orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.FULL, PAGE_SIZE))
                    .hasMessageContaining("SIMULATED_SIGKILL_CRASH_1_BEFORE_CHECKPOINT");

            assertThatThrownBy(() -> orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.RESUME, PAGE_SIZE))
                    .hasMessageContaining("SIMULATED_SIGKILL_CRASH_2_BEFORE_CHECKPOINT");

            orchestrator.setFaultHook(FaultInjectionHook.NO_OP);
            SyncRunInfo completedRun = orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.RESUME, PAGE_SIZE);
            assertThat(completedRun.status()).isEqualTo("COMPLETED");

            awaitRowCount("rdw_vehicles", FIXTURE_RECORDS, Duration.ofSeconds(30));
            Thread.sleep(150L);

            long actualRows = queryRowCount("rdw_vehicles");
            String actualChecksum = queryChecksum("rdw_vehicles");

            assertThat(actualRows)
                    .as("Run %d must have exact row count with 0 duplicates and 0 gaps", run)
                    .isEqualTo(FIXTURE_RECORDS);
            assertThat(actualChecksum)
                    .as("Run %d content checksum must match clean baseline", run)
                    .isEqualTo(expectedChecksum);
        }
    }

    private void awaitRowCount(String table, long expected, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (queryRowCount(table) >= expected) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Timed out waiting for " + expected + " rows in " + table);
    }

    private long queryRowCount(String table) {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
        return count != null ? count : 0L;
    }

    private String queryChecksum(String table) {
        return jdbcTemplate.queryForObject(
                "SELECT md5(string_agg(pk || ':' || source_hash, ',' ORDER BY pk)) FROM " + table,
                String.class);
    }
}
