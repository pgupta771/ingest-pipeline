package dev.ingest.core;

import java.util.List;
import java.util.Objects;

/**
 * Declares the baseline schema and primary key of an extracted stream.
 *
 * @param stream     unique stream name
 * @param primaryKey primary key field name
 * @param columns    ordered list of known columns
 */
public record SchemaDescriptor(String stream, String primaryKey, List<ColumnSpec> columns) {

    public SchemaDescriptor {
        Objects.requireNonNull(stream, "stream must not be null");
        Objects.requireNonNull(primaryKey, "primaryKey must not be null");
        Objects.requireNonNull(columns, "columns must not be null");
        columns = List.copyOf(columns);
    }
}
