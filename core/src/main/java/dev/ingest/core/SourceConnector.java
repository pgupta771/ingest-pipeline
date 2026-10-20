package dev.ingest.core;

import java.util.List;
import java.util.Optional;

/**
 * Service Provider Interface (SPI) implemented by all source connectors.
 */
public interface SourceConnector {

    /**
     * Unique connector identifier (e.g., {@code "rdw"}, {@code "ecb-xml"}).
     */
    String id();

    /**
     * Returns all streams supported by this connector.
     */
    List<StreamDescriptor> discoverStreams();

    /**
     * Returns the baseline schema descriptor for the given stream name.
     */
    SchemaDescriptor describe(String stream);

    /**
     * Opens a resumable extraction session starting immediately after {@code cursor}.
     */
    SourceSession openSession(String stream, Optional<Cursor> cursor);
}
