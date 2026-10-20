package dev.ingest.sink.postgres;

import dev.ingest.core.SourceRecord;

/**
 * Pairs a deserialized {@link SourceRecord} with its exact Kafka broker coordinates.
 */
public record ConsumedRecordEnvelope(
        SourceRecord record,
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset) {
}
