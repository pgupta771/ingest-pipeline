package dev.ingest.engine;

import java.time.Instant;
import java.util.UUID;

/**
 * Snapshot of a synchronization run stored in {@code sync_runs}.
 */
public record SyncRunInfo(
        UUID id,
        String connector,
        String stream,
        String mode,
        String status,
        Instant startedAt,
        Instant finishedAt,
        long recordsExtracted,
        long recordsLoaded,
        long recordsDead,
        String errorMessage) {
}
