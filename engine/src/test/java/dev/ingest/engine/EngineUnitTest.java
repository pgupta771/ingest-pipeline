package dev.ingest.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.kafka.core.KafkaTemplate;

class EngineUnitTest {

    @Test
    void orchestratorExtractsPagesAndMarksCompletedWhenExhausted() {
        ConnectorRegistry registry = new ConnectorRegistry();
        registry.register(singlePageConnector(false));

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), any()))
                .thenReturn(List.of("k0"))
                .thenReturn(List.of(new SyncRunInfo(
                        UUID.randomUUID(), "fake", "s1", "FULL", "COMPLETED",
                        Instant.now(), Instant.now(), 1L, 1L, 0L, null)));

        CheckpointRepository repo = new CheckpointRepository(jdbc);

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
        verify(kafka).send(anyString(), anyString(), anyString());
    }

    @Test
    void orchestratorMarksSyncInterruptedWhenStoppedBeforeLastPage() {
        ConnectorRegistry registry = new ConnectorRegistry();
        registry.register(singlePageConnector(true));

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        CheckpointRepository repo = new CheckpointRepository(jdbc) {
            @Override
            public Optional<Cursor> findCheckpoint(String stream) {
                return Optional.empty();
            }

            @Override
            public Optional<SyncRunInfo> findSyncRun(UUID runId) {
                return Optional.of(new SyncRunInfo(
                        runId, "fake", "s1", "RESUME", "INTERRUPTED",
                        Instant.now(), Instant.now(), 1L, 0L, 0L, null));
            }
        };

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(
                registry, repo, kafka, new SimpleMeterRegistry());
        orchestrator.setFaultHook((stream, page, total) -> orchestrator.stop());

        SyncRunInfo info = orchestrator.runSync("fake", "s1", SyncMode.fromString(""), 100);
        assertThat(info.status()).isEqualTo("INTERRUPTED");
        verify(jdbc).update(
                eq("UPDATE sync_runs SET status = 'INTERRUPTED', finished_at = NOW() WHERE id = ?"),
                any(UUID.class));
    }

    @Test
    void orchestratorMarksSyncFailedOnException() {
        ConnectorRegistry registry = new ConnectorRegistry();
        registry.register(singlePageConnector(false));

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        CheckpointRepository repo = new CheckpointRepository(jdbc) {
            @Override
            public Optional<Cursor> findCheckpoint(String stream) {
                return Optional.empty();
            }
        };

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        ExtractionOrchestrator orchestrator = new ExtractionOrchestrator(
                registry, repo, kafka, new SimpleMeterRegistry());
        orchestrator.setFaultHook((s, p, t) -> {
            throw new IllegalStateException("simulated failure");
        });

        assertThatThrownBy(() -> orchestrator.runSync("fake", "s1", SyncMode.RESUME, 10))
                .isInstanceOf(IllegalStateException.class);
    }

    private static SourceConnector singlePageConnector(boolean hasMore) {
        return new SourceConnector() {
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
                AtomicInteger calls = new AtomicInteger();
                return pageSize -> {
                    if (calls.incrementAndGet() == 1) {
                        return new Page(
                                List.of(SourceRecord.create(stream, "k1", Map.of("id", "k1"), Instant.now())),
                                Optional.of(Cursor.of("k1")),
                                hasMore);
                    }
                    return Page.empty(Optional.of(Cursor.of("k1")));
                };
            }
        };
    }
}
