package dev.ingest.core;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe lookup registry of available {@link SourceConnector} implementations.
 */
public final class ConnectorRegistry {

    private final Map<String, SourceConnector> connectors = new ConcurrentHashMap<>();

    public void register(SourceConnector connector) {
        Objects.requireNonNull(connector, "connector must not be null");
        connectors.put(connector.id(), connector);
    }

    public Optional<SourceConnector> find(String connectorId) {
        return Optional.ofNullable(connectors.get(connectorId));
    }

    public SourceConnector getRequired(String connectorId) {
        return find(connectorId).orElseThrow(() ->
                new IllegalArgumentException("Unknown connector: " + connectorId));
    }

    public Collection<SourceConnector> listAll() {
        return Collections.unmodifiableCollection(connectors.values());
    }
}
