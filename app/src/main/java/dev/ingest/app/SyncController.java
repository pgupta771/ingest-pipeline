package dev.ingest.app;

import dev.ingest.core.ConnectorRegistry;
import dev.ingest.core.StreamDescriptor;
import dev.ingest.engine.CheckpointRepository;
import dev.ingest.engine.ExtractionOrchestrator;
import dev.ingest.engine.SyncMode;
import dev.ingest.engine.SyncRunInfo;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Minimal REST API for triggering sync runs and inspecting stream metadata.
 */
@RestController
@RequestMapping("/api/v1")
public class SyncController {

    private final ExtractionOrchestrator orchestrator;
    private final CheckpointRepository checkpointRepository;
    private final ConnectorRegistry connectorRegistry;

    public SyncController(
            ExtractionOrchestrator orchestrator,
            CheckpointRepository checkpointRepository,
            ConnectorRegistry connectorRegistry) {
        this.orchestrator = orchestrator;
        this.checkpointRepository = checkpointRepository;
        this.connectorRegistry = connectorRegistry;
    }

    public record StartSyncRequest(String connector, String stream, String mode, Integer pageSize) {}

    @PostMapping("/syncs")
    public ResponseEntity<SyncRunInfo> startSync(@RequestBody StartSyncRequest request) {
        int pageSize = (request.pageSize() != null && request.pageSize() > 0) ? request.pageSize() : 2000;
        SyncRunInfo info = orchestrator.runSync(
                request.connector(),
                request.stream(),
                SyncMode.fromString(request.mode()),
                pageSize);
        return ResponseEntity.ok(info);
    }

    @GetMapping("/syncs/{id}")
    public ResponseEntity<SyncRunInfo> getSync(@PathVariable UUID id) {
        return checkpointRepository.findSyncRun(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/streams")
    public List<StreamDescriptor> listStreams() {
        return connectorRegistry.listAll().stream()
                .flatMap(c -> c.discoverStreams().stream())
                .toList();
    }
}
