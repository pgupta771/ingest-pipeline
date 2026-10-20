package dev.ingest.app;

import dev.ingest.app.fixture.RdwFixtureServer;
import dev.ingest.connector.ecb.EcbXmlConnector;
import dev.ingest.connector.rdw.RdwConnectorConfig;
import dev.ingest.connector.rdw.RdwVehicleConnector;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.core.SourceRecord;
import dev.ingest.engine.ExtractionOrchestrator;
import dev.ingest.engine.FaultInjectionHook;
import dev.ingest.engine.SyncMode;
import dev.ingest.sink.postgres.ConsumedRecordEnvelope;
import dev.ingest.sink.postgres.PostgresTransactionalWriter;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
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
 * Reproducible benchmark runner invoked by {@code scripts/benchmark.sh} to measure:
 * (1) PostgreSQL upsert throughput across batch sizes 100 / 500 / 1000 / 5000,
 * (2) End-to-end RDW JSON extract -> Kafka -> Postgres throughput,
 * (3) End-to-end ECB XML extract -> Kafka -> Postgres throughput, and
 * (4) Mid-sync crash-recovery convergence latency.
 */
@SpringBootTest(classes = IngestPipelineApplication.class)
@Testcontainers
@EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
class BenchmarkSuiteIT {

    @TempDir
    Path tempDir;

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
    private PostgresTransactionalWriter writer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void runAndExportAllBenchmarks() throws Exception {
        int totalBatchRecords = 10_000;
        List<ConsumedRecordEnvelope> prebuilt = new ArrayList<>(totalBatchRecords);
        Instant now = Instant.now();
        for (int i = 1; i <= totalBatchRecords; i++) {
            String pk = String.format("BM%06d", i);
            SourceRecord sr = SourceRecord.create("rdw_vehicles", pk, Map.of(
                    "kenteken", pk,
                    "voertuigsoort", "Personenauto",
                    "merk", "VOLVO",
                    "handelsbenaming", "XC40",
                    "eerste_kleur", "ZWART",
                    "datum_eerste_toelating", "20230101",
                    "massa_ledig_voertuig", 1600), now);
            prebuilt.add(new ConsumedRecordEnvelope(sr, "ingest.raw.rdw_vehicles", 0, i));
        }

        long rps100 = measureBatchSize(prebuilt, 100);
        long rps500 = measureBatchSize(prebuilt, 500);
        long rps1000 = measureBatchSize(prebuilt, 1000);
        long rps5000 = measureBatchSize(prebuilt, 5000);

        // End-to-end RDW JSON (20,000 records)
        int rdwCount = 20_000;
        long rdwE2eRps;
        try (RdwFixtureServer server = new RdwFixtureServer(rdwCount)) {
            jdbcTemplate.execute("TRUNCATE TABLE rdw_vehicles");
            connectorRegistry.register(new RdwVehicleConnector(RdwConnectorConfig.defaults(server.baseUri())));
            long t0 = System.nanoTime();
            orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.FULL, 2500);
            awaitRows("rdw_vehicles", rdwCount, Duration.ofSeconds(60));
            long elapsedMs = Math.max(1L, (System.nanoTime() - t0) / 1_000_000L);
            rdwE2eRps = (rdwCount * 1000L) / elapsedMs;
        }

        // End-to-end ECB XML (20,000 records)
        int ecbDays = 2_000; // 2,000 days * 10 currencies = 20,000 records
        int ecbCount = ecbDays * 10;
        Path xmlFile = tempDir.resolve("ecb-e2e.xml");
        writeEcbXml(xmlFile, ecbDays);
        connectorRegistry.register(new EcbXmlConnector(xmlFile));
        long t1 = System.nanoTime();
        orchestrator.runSync("ecb-xml", "ecb_exchange_rates", SyncMode.FULL, 2500);
        awaitRows("ecb_exchange_rates", ecbCount, Duration.ofSeconds(60));
        long ecbElapsedMs = Math.max(1L, (System.nanoTime() - t1) / 1_000_000L);
        long ecbE2eRps = (ecbCount * 1000L) / ecbElapsedMs;

        // Crash recovery time on 10,000 RDW records with mid-sync crash at page 3
        long recoveryMs;
        try (RdwFixtureServer server = new RdwFixtureServer(10_000)) {
            jdbcTemplate.execute("TRUNCATE TABLE rdw_vehicles");
            connectorRegistry.register(new RdwVehicleConnector(RdwConnectorConfig.defaults(server.baseUri())));
            Set<Integer> fired = ConcurrentHashMap.newKeySet();
            orchestrator.setFaultHook((s, page, total) -> {
                if (page == 3 && fired.add(1)) {
                    throw new RuntimeException("BENCHMARK_CRASH");
                }
            });
            try {
                orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.FULL, 2000);
            } catch (RuntimeException ignored) {
            }
            orchestrator.setFaultHook(FaultInjectionHook.NO_OP);
            long r0 = System.nanoTime();
            orchestrator.runSync("rdw", "rdw_vehicles", SyncMode.RESUME, 2000);
            awaitRows("rdw_vehicles", 10_000, Duration.ofSeconds(30));
            recoveryMs = Math.max(1L, (System.nanoTime() - r0) / 1_000_000L);
        }

        String results = String.format(
                "BATCH_100_RPS=%d\nBATCH_500_RPS=%d\nBATCH_1000_RPS=%d\nBATCH_5000_RPS=%d\nRDW_E2E_RPS=%d\nECB_E2E_RPS=%d\nCRASH_RECOVERY_MS=%d\n",
                rps100, rps500, rps1000, rps5000, rdwE2eRps, ecbE2eRps, recoveryMs);
        Files.writeString(Path.of("/tmp/ingest-benchmark-results.env"), results, StandardCharsets.UTF_8);
    }

    private long measureBatchSize(List<ConsumedRecordEnvelope> records, int batchSize) {
        jdbcTemplate.execute("DROP TABLE IF EXISTS rdw_vehicles");
        jdbcTemplate.execute("DELETE FROM stream_schema WHERE stream = 'rdw_vehicles'");
        long startNs = System.nanoTime();
        for (int i = 0; i < records.size(); i += batchSize) {
            List<ConsumedRecordEnvelope> slice = records.subList(i, Math.min(records.size(), i + batchSize));
            writer.writeBatchTransactionally(
                    "rdw_vehicles",
                    slice,
                    List.of(),
                    Map.of(new TopicPartition("ingest.raw.rdw_vehicles", 0), (long) (i + slice.size())));
        }
        long elapsedMs = Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L);
        return (records.size() * 1000L) / elapsedMs;
    }

    private void awaitRows(String table, long expected, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                Long c = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
                if (c != null && c >= expected) {
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Timed out waiting for " + expected + " rows in " + table);
    }

    private static void writeEcbXml(Path target, int days) throws Exception {
        String[] curr = {"USD", "GBP", "JPY", "CHF", "CAD", "AUD", "SEK", "NOK", "DKK", "PLN"};
        try (BufferedWriter w = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
            w.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<gesmes:Envelope xmlns:gesmes=\"http://www.gesmes.org/xml/2002-08-01\">\n<Cube>\n");
            for (int d = 1; d <= days; d++) {
                w.write(String.format("  <Cube time=\"2026-%02d-%02d-%05d\">\n", (d % 12) + 1, (d % 28) + 1, d));
                for (int c = 0; c < curr.length; c++) {
                    w.write(String.format("    <Cube currency=\"%s\" rate=\"1.%04d\"/>\n", curr[c], 1000 + ((d + c) % 8000)));
                }
                w.write("  </Cube>\n");
            }
            w.write("</Cube>\n</gesmes:Envelope>\n");
        }
    }
}
