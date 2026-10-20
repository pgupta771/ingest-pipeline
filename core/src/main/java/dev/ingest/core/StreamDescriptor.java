package dev.ingest.core;

import java.util.Objects;

/**
 * Metadata describing an available data stream exposed by a {@link SourceConnector}.
 *
 * @param name        stream identifier
 * @param description human-readable summary
 * @param schema      initial schema descriptor
 */
public record StreamDescriptor(String name, String description, SchemaDescriptor schema) {

    public StreamDescriptor {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(description, "description must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
    }
}
