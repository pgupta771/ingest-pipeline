package dev.ingest.sink.postgres;

/**
 * Represents a poison or schema-incompatible record routed to {@code dead_letters}.
 */
public record DeadLetterEntry(
        String stream,
        String primaryKey,
        String reasonCode,
        String reasonDetail,
        String rawPayload,
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset) {
}
