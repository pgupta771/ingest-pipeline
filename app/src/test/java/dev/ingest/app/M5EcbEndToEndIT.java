package dev.ingest.app;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ingest.connector.ecb.EcbXmlConnector;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.engine.ExtractionOrchestrator;
import dev.ingest.engine.SyncMode;
import dev.ingest.engine.SyncRunInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
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
 * End-to-End Integration Test for ECB XML Connector -> Kafka -> PostgreSQL (`ecb_exchange_rates`).
 */
@SpringBootTest(classes = IngestPipelineApplication.class)
@Testcontainers
class M5EcbEndToEndIT {

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
    private JdbcTemplate jdbcTemplate;

    @Test
    void extractsEcbXmlThroughKafkaIntoPostgresAndUpdatesSyncRunCounters() throws Exception {
        Path xml = tempDir.resolve("ecb-sample.xml");
        Files.writeString(xml, """
                <?xml version="1.0" encoding="UTF-8"?>
                <gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
                                 xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
                  <Cube>
                    <Cube time="2026-10-19">
                      <Cube currency="USD" rate="1.0850"/>
                      <Cube currency="GBP" rate="0.8420"/>
                    </Cube>
                    <Cube time="2026-10-20">
                      <Cube currency="USD" rate="1.0910"/>
                      <Cube currency="JPY" rate="162.45"/>
                    </Cube>
                  </Cube>
                </gesmes:Envelope>
                """);

        connectorRegistry.register(new EcbXmlConnector(xml));

        SyncRunInfo run = orchestrator.runSync("ecb-xml", "ecb_exchange_rates", SyncMode.FULL, 2);
        assertThat(run.status()).isEqualTo("COMPLETED");
        assertThat(run.recordsExtracted()).isEqualTo(4L);

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ecb_exchange_rates'",
                    Integer.class);
            if (count != null && count > 0) {
                Long rows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ecb_exchange_rates", Long.class);
                if (rows != null && rows == 4L) {
                    break;
                }
            }
            Thread.sleep(100L);
        }

        Long rows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ecb_exchange_rates", Long.class);
        Long loadedInSyncRuns = jdbcTemplate.queryForObject(
                "SELECT records_loaded FROM sync_runs WHERE id = ?", Long.class, run.id());

        assertThat(rows).isEqualTo(4L);
        assertThat(loadedInSyncRuns).isEqualTo(4L);
    }
}
