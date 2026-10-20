package dev.ingest.connector.rdw;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Configuration for the Dutch RDW Socrata SODA connector.
 *
 * @param baseUri        base API URI (e.g., {@code https://opendata.rdw.nl})
 * @param datasetId      Socrata dataset identifier (default {@code m9d7-ebf2})
 * @param appToken       optional Socrata {@code X-App-Token}
 * @param requestTimeout HTTP timeout per request
 * @param maxAttempts    maximum retry attempts on 429/5xx/network failure
 * @param initialBackoff initial retry backoff duration
 */
public record RdwConnectorConfig(
        URI baseUri,
        String datasetId,
        Optional<String> appToken,
        Duration requestTimeout,
        int maxAttempts,
        Duration initialBackoff) {

    public static final String DEFAULT_DATASET_ID = "m9d7-ebf2";

    public RdwConnectorConfig {
        Objects.requireNonNull(baseUri, "baseUri must not be null");
        Objects.requireNonNull(datasetId, "datasetId must not be null");
        Objects.requireNonNull(appToken, "appToken must not be null");
        Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        Objects.requireNonNull(initialBackoff, "initialBackoff must not be null");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
    }

    public static RdwConnectorConfig defaults(URI baseUri) {
        return new RdwConnectorConfig(
                baseUri,
                DEFAULT_DATASET_ID,
                Optional.empty(),
                Duration.ofSeconds(15),
                4,
                Duration.ofMillis(200));
    }
}
