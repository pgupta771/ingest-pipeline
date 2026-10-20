package dev.ingest.engine;

/**
 * Execution mode for a stream synchronization run.
 */
public enum SyncMode {
    FULL,
    RESUME;

    public static SyncMode fromString(String value) {
        if (value == null || value.isBlank()) {
            return RESUME;
        }
        return SyncMode.valueOf(value.trim().toUpperCase());
    }
}
