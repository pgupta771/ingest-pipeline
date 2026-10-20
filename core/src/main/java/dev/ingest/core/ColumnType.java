package dev.ingest.core;

/**
 * Supported canonical column types across source schemas and the PostgreSQL sink.
 */
public enum ColumnType {
    BOOLEAN,
    INTEGER,
    BIGINT,
    NUMERIC,
    TIMESTAMPTZ,
    TEXT,
    JSONB;

    /**
     * Returns true if widening from this type to {@code target} is lossless and permitted.
     */
    public boolean canWidenTo(ColumnType target) {
        if (this == target) {
            return true;
        }
        return switch (this) {
            case INTEGER -> target == BIGINT || target == NUMERIC;
            case BIGINT -> target == NUMERIC;
            default -> false;
        };
    }
}
