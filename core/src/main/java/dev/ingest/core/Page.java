package dev.ingest.core;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A single batch of records returned by a {@link SourceSession} along with its continuation cursor.
 *
 * @param records    extracted records for this page
 * @param nextCursor updated cursor to persist after the page is durably published
 * @param hasMore    true if additional pages remain in the stream
 */
public record Page(List<SourceRecord> records, Optional<Cursor> nextCursor, boolean hasMore) {

    public Page {
        Objects.requireNonNull(records, "records must not be null");
        Objects.requireNonNull(nextCursor, "nextCursor must not be null");
        records = List.copyOf(records);
    }

    public static Page empty(Optional<Cursor> lastCursor) {
        return new Page(List.of(), lastCursor, false);
    }
}
