package dev.ingest.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.ingest.core.ColumnSpec;
import dev.ingest.core.ColumnType;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.core.Cursor;
import dev.ingest.core.Page;
import dev.ingest.core.SchemaDescriptor;
import dev.ingest.core.SourceConnector;
import dev.ingest.core.SourceRecord;
import dev.ingest.core.SourceSession;
import dev.ingest.core.StreamDescriptor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

class EngineUnitTest {

    @Test
    void orchestratorExtractsPagesAndSavesCheckpointsAfterAcks() {
        ConnectorRegistry registry = new ConnectorRegistry();
        SourceConnector fakeConnector = new SourceConnector() {
            @Override
            public String id() {
                return "fake";
            }

            @Override
            public List<StreamDescriptor> discoverStreams() {
                return List.of();
            }

            @Override
            public SchemaDescriptor describe(String stream) {
                return new SchemaDescriptor(stream, "id", List.of(new ColumnSpec("id", ColumnType.TEXT, false)));
            }

            @Override
            public SourceSession openSession(String stream, Optional<Cursor> cursor) {
                return pageSize -> new Page(
                        List.of(SourceRecord.create(stream, "k1", Map.of("id", "k1"), Instant.now())),
                        Optional.of(Cursor.of("k1")),
                        false);
            }
        };
        registry.register(fakeConnector);

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        CheckpointRepository repo = new CheckpointRepository(jdbc) {
            @Override
            public Optional<Cursor> findCheckpoint(String stream) {
                return Optional.empty();
            }

            @Override
            public Optional<SyncRunInfo> findSyncRun(UUID runId) {
                return Optional.of(new SyncRunInfo(
                        runId, "fake", "s1", "FULL", "COMPLETED", Instant.now(), Instant.now(), 1L, 0L, 0L, null));
            }
        };

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(
                registry, repo, kafka, new SimpleMeterRegistry());
        orchestrator.start();
        assertThat(orchestrator.isRunning()).isTrue();

        SyncRunInfo result = orchestrator.runSync("fake", "s1", SyncMode.fromString("full"), 100);
        assertThat(result.status()).isEqualTo("COMPLETED");
        assertThat(result.recordsExtracted()).isEqualTo(1L);
        verify(kafka).send(anyString(), anyString(), anyString());

        orchestrator.stop();
        assertThat(orchestrator.isRunning()).isFalse();
    }
}
