package dev.ingest.engine;

/**
 * Hook invoked at critical pipeline boundaries to allow deterministic crash-recovery tests.
 */
@FunctionalInterface
public interface FaultInjectionHook {

    FaultInjectionHook NO_OP = (stream, pageNumber, recordsPublished) -> {};

    /**
     * Called immediately after all Kafka broker acks arrive for a page, before {@code extract_checkpoint} is updated.
     */
    void afterKafkaAckBeforeCheckpoint(String stream, int pageNumber, long recordsPublished);
}
