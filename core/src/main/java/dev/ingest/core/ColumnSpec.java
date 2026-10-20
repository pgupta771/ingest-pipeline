package dev.ingest.core;

import java.util.Objects;

/**
 * Describes a single column in a stream schema.
 *
 * @param name     SQL-safe column identifier
 * @param type     canonical column data type
 * @param nullable whether NULL values are allowed
 */
public record ColumnSpec(String name, ColumnType type, boolean nullable) {

    public ColumnSpec {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("column name must not be blank");
        }
    }
}
