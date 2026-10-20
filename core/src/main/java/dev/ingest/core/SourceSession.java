package dev.ingest.core;

/**
 * Stateful, closable extraction session over a stream.
 * Allows file/stream parsers (such as StAX XML) to keep an open cursor in O(1) memory across pages.
 */
public interface SourceSession extends AutoCloseable {

    /**
     * Fetches the next page of up to {@code pageSize} records.
     *
     * @param pageSize maximum records to return (must be {@code > 0})
     * @return extracted page and updated cursor
     */
    Page nextPage(int pageSize);

    @Override
    default void close() {
        // Default no-op for stateless HTTP sessions
    }
}
