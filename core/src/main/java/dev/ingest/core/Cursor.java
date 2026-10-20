package dev.ingest.core;

import java.util.Objects;

/**
 * Opaque, serializable checkpoint cursor tracking extraction progress for a stream.
 *
 * @param value non-blank cursor string (e.g., last primary key or file offset)
 */
public record Cursor(String value) {

    public Cursor {
        Objects.requireNonNull(value, "cursor value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("cursor value must not be blank");
        }
    }

    public static Cursor of(String value) {
        return new Cursor(value);
    }
}
