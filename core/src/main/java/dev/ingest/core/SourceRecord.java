package dev.ingest.core;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable data record extracted from a source stream.
 *
 * @param stream      stream name
 * @param primaryKey  non-blank business key
 * @param payload     field-value map
 * @param extractedAt UTC extraction timestamp
 * @param sourceHash  SHA-256 hex digest of the payload
 */
public record SourceRecord(
        String stream,
        String primaryKey,
        Map<String, Object> payload,
        Instant extractedAt,
        String sourceHash) {

    public SourceRecord {
        Objects.requireNonNull(stream, "stream must not be null");
        Objects.requireNonNull(primaryKey, "primaryKey must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(extractedAt, "extractedAt must not be null");
        Objects.requireNonNull(sourceHash, "sourceHash must not be null");
        if (primaryKey.isBlank()) {
            throw new IllegalArgumentException("primaryKey must not be blank");
        }
        payload = Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }

    /**
     * Creates a {@link SourceRecord} and automatically computes its canonical SHA-256 {@code sourceHash}.
     */
    public static SourceRecord create(
            String stream,
            String primaryKey,
            Map<String, Object> payload,
            Instant extractedAt) {
        return new SourceRecord(
                stream,
                primaryKey,
                payload,
                extractedAt,
                PayloadHasher.sha256(payload));
    }
}
