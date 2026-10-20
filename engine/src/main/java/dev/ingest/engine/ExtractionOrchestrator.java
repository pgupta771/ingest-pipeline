package dev.ingest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.core.Cursor;
import dev.ingest.core.Page;
import dev.ingest.core.SourceConnector;
import dev.ingest.core.SourceRecord;
import dev.ingest.core.SourceSession;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Orchestrates paginated extraction from a {@link SourceConnector} into Kafka and persists checkpoints.
 */
@Service
public class ExtractionOrchestrator implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ExtractionOrchestrator.class);

    private final ConnectorRegistry connectorRegistry;
    private final CheckpointRepository checkpointRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile FaultInjectionHook faultHook = FaultInjectionHook.NO_OP;

    @Autowired
    public ExtractionOrchestrator(
            ConnectorRegistry connectorRegistry,
            CheckpointRepository checkpointRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            MeterRegistry meterRegistry) {
        this.connectorRegistry = connectorRegistry;
        this.checkpointRepository = checkpointRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void setFaultHook(FaultInjectionHook faultHook) {
        this.faultHook = faultHook != null ? faultHook : FaultInjectionHook.NO_OP;
    }

    /**
     * Runs a synchronous extraction loop for the given connector and stream.
     */
    public SyncRunInfo runSync(String connectorId, String stream, SyncMode mode, int pageSize) {
        SourceConnector connector = connectorRegistry.getRequired(connectorId);
        if (mode == SyncMode.FULL) {
            checkpointRepository.deleteCheckpoint(stream);
        }

        Optional<Cursor> startCursor = checkpointRepository.findCheckpoint(stream);
        UUID runId = checkpointRepository.createSyncRun(connectorId, stream, mode);
        String topic = "ingest.raw." + stream;
        Counter extractedCounter = Counter.builder("ingest_records_extracted_total")
                .tag("connector", connectorId)
                .tag("stream", stream)
                .register(meterRegistry);

        log.info("Starting sync runId={} connector={} stream={} mode={} startCursor={}",
                runId, connectorId, stream, mode, startCursor.map(Cursor::value).orElse("<BEGIN>"));

        int pageNumber = 0;
        long totalPublished = 0L;

        try (SourceSession session = connector.openSession(stream, startCursor)) {
            while (running.get()) {
                Page page = session.nextPage(pageSize);
                List<SourceRecord> records = page.records();
                if (!records.isEmpty()) {
                    pageNumber++;
                    publishPageAndAwaitAcks(topic, records);
                    totalPublished += records.size();
                    extractedCounter.increment(records.size());

                    faultHook.afterKafkaAckBeforeCheckpoint(stream, pageNumber, totalPublished);

                    if (page.nextCursor().isPresent()) {
                        checkpointRepository.saveCheckpoint(stream, page.nextCursor().get(), runId);
                    }
                    checkpointRepository.incrementExtracted(runId, records.size());
                }

                if (!page.hasMore()) {
                    break;
                }
            }
            checkpointRepository.markSyncCompleted(runId);
            log.info("Completed sync runId={} stream={} totalPublished={}", runId, stream, totalPublished);
        } catch (Exception e) {
            log.error("Sync failed runId={} stream={}: {}", runId, stream, e.getMessage(), e);
            checkpointRepository.markSyncFailed(runId, e.getMessage());
            throw e;
        }

        return checkpointRepository.findSyncRun(runId).orElseThrow();
    }

    private void publishPageAndAwaitAcks(String topic, List<SourceRecord> records) {
        List<CompletableFuture<?>> futures = new ArrayList<>(records.size());
        for (SourceRecord record : records) {
            try {
                String json = objectMapper.writeValueAsString(record);
                futures.add(kafkaTemplate.send(topic, record.primaryKey(), json));
            } catch (Exception e) {
                throw new IllegalStateException("Failed to serialize or send record " + record.primaryKey(), e);
            }
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        log.info("Graceful shutdown requested; extractor will stop after completing the current page.");
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
