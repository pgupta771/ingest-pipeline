package dev.ingest.connector.rdw;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ingest.core.ColumnSpec;
import dev.ingest.core.ColumnType;
import dev.ingest.core.ConnectorException;
import dev.ingest.core.Cursor;
import dev.ingest.core.Page;
import dev.ingest.core.SchemaDescriptor;
import dev.ingest.core.SourceConnector;
import dev.ingest.core.SourceRecord;
import dev.ingest.core.SourceSession;
import dev.ingest.core.StreamDescriptor;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts registered vehicle records from the Dutch RDW Open Data Socrata SODA API
 * using keyset pagination ordered on {@code kenteken}.
 */
public class RdwVehicleConnector implements SourceConnector {

    public static final String CONNECTOR_ID = "rdw";
    public static final String STREAM_NAME = "rdw_vehicles";
    public static final String PRIMARY_KEY = "kenteken";

    private static final Logger log = LoggerFactory.getLogger(RdwVehicleConnector.class);
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAP = new TypeReference<>() {};

    private final RdwConnectorConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final CircuitBreaker circuitBreaker;
    private final AtomicLong retryCount = new AtomicLong();

    public RdwVehicleConnector(RdwConnectorConfig config) {
        this(
                config,
                HttpClient.newBuilder().connectTimeout(config.requestTimeout()).build(),
                new ObjectMapper(),
                CircuitBreaker.of("rdw-api", CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .minimumNumberOfCalls(4)
                        .slidingWindowSize(6)
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        .recordException(ex -> ex instanceof ConnectorException ce && ce.isRetryable())
                        .build()));
    }

    public RdwVehicleConnector(
            RdwConnectorConfig config,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            CircuitBreaker circuitBreaker) {
        this.config = config;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public String id() {
        return CONNECTOR_ID;
    }

    @Override
    public List<StreamDescriptor> discoverStreams() {
        return List.of(new StreamDescriptor(
                STREAM_NAME,
                "Dutch RDW registered vehicles (Gekentekende voertuigen, m9d7-ebf2)",
                describe(STREAM_NAME)));
    }

    @Override
    public SchemaDescriptor describe(String stream) {
        if (!STREAM_NAME.equals(stream)) {
            throw new IllegalArgumentException("Unsupported stream for RDW connector: " + stream);
        }
        return new SchemaDescriptor(
                STREAM_NAME,
                PRIMARY_KEY,
                List.of(
                        new ColumnSpec("kenteken", ColumnType.TEXT, false),
                        new ColumnSpec("voertuigsoort", ColumnType.TEXT, true),
                        new ColumnSpec("merk", ColumnType.TEXT, true),
                        new ColumnSpec("handelsbenaming", ColumnType.TEXT, true),
                        new ColumnSpec("eerste_kleur", ColumnType.TEXT, true),
                        new ColumnSpec(" datum_eerste_toelating".trim(), ColumnType.TEXT, true),
                        new ColumnSpec("massa_ledig_voertuig", ColumnType.INTEGER, true)));
    }

    @Override
    public SourceSession openSession(String stream, Optional<Cursor> cursor) {
        if (!STREAM_NAME.equals(stream)) {
            throw new IllegalArgumentException("Unsupported stream for RDW connector: " + stream);
        }
        return new RdwSourceSession(cursor);
    }

    public long totalRetries() {
        return retryCount.get();
    }

    private final class RdwSourceSession implements SourceSession {

        private Optional<Cursor> currentCursor;
        private boolean exhausted;

        private RdwSourceSession(Optional<Cursor> initialCursor) {
            this.currentCursor = initialCursor;
            this.exhausted = false;
        }

        @Override
        public Page nextPage(int pageSize) {
            if (pageSize <= 0) {
                throw new IllegalArgumentException("pageSize must be > 0");
            }
            if (exhausted) {
                return Page.empty(currentCursor);
            }

            URI uri = buildKeysetUri(currentCursor, pageSize);
            String body = executeWithRetry(uri);
            List<Map<String, Object>> rawRows = parseJsonRows(body);

            if (rawRows.isEmpty()) {
                exhausted = true;
                return Page.empty(currentCursor);
            }

            Instant extractedAt = Instant.now();
            List<SourceRecord> deduplicated = new ArrayList<>(rawRows.size());
            String highWatermarkKey = currentCursor.map(Cursor::value).orElse(null);

            for (Map<String, Object> row : rawRows) {
                Object rawKey = row.get(PRIMARY_KEY);
                if (rawKey == null || rawKey.toString().isBlank()) {
                    continue;
                }
                String pk = rawKey.toString();
                if (highWatermarkKey != null && pk.compareTo(highWatermarkKey) <= 0) {
                    log.debug("Skipping duplicate or out-of-order key {} <= {}", pk, highWatermarkKey);
                    continue;
                }
                highWatermarkKey = pk;
                deduplicated.add(SourceRecord.create(STREAM_NAME, pk, row, extractedAt));
            }

            if (highWatermarkKey != null) {
                currentCursor = Optional.of(Cursor.of(highWatermarkKey));
            }

            boolean hasMore = rawRows.size() >= pageSize && !deduplicated.isEmpty();
            if (!hasMore) {
                exhausted = true;
            }
            return new Page(deduplicated, currentCursor, hasMore);
        }
    }

    private URI buildKeysetUri(Optional<Cursor> cursor, int pageSize) {
        StringBuilder query = new StringBuilder();
        query.append("$limit=").append(pageSize);
        query.append("&$order=").append(urlEncode(PRIMARY_KEY + " ASC"));
        cursor.ifPresent(c -> {
            String escapedKey = c.value().replace("'", "''");
            String whereClause = PRIMARY_KEY + " > '" + escapedKey + "'";
            query.append("&$where=").append(urlEncode(whereClause));
        });
        String base = config.baseUri().toString().replaceAll("/+$", "");
        return URI.create(base + "/resource/" + config.datasetId() + ".json?" + query);
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String executeWithRetry(URI uri) {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return circuitBreaker.executeCallable(() -> sendSingleRequest(uri));
            } catch (CallNotPermittedException openEx) {
                throw new ConnectorException("CIRCUIT_OPEN", "Circuit breaker is open for RDW API", true, openEx);
            } catch (RateLimitException rateEx) {
                if (attempt >= config.maxAttempts()) {
                    throw new ConnectorException("RATE_LIMITED", rateEx.getMessage(), true, rateEx);
                }
                retryCount.incrementAndGet();
                sleepQuietly(rateEx.retryAfter());
            } catch (ConnectorException ce) {
                if (!ce.isRetryable() || attempt >= config.maxAttempts()) {
                    throw ce;
                }
                retryCount.incrementAndGet();
                sleepQuietly(computeBackoffWithJitter(attempt));
            } catch (Exception ex) {
                if (attempt >= config.maxAttempts()) {
                    throw new ConnectorException("HTTP_ERROR", "RDW request failed after retries", true, ex);
                }
                retryCount.incrementAndGet();
                sleepQuietly(computeBackoffWithJitter(attempt));
            }
        }
    }

    private String sendSingleRequest(URI uri) {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder(uri)
                .GET()
                .timeout(config.requestTimeout())
                .header("Accept", "application/json");
        config.appToken().ifPresent(token -> reqBuilder.header("X-App-Token", token));

        try {
            HttpResponse<String> response = httpClient.send(
                    reqBuilder.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int status = response.statusCode();
            if (status == 200) {
                return response.body();
            }
            if (status == 429) {
                Duration wait = parseRetryAfter(response.headers().firstValue("Retry-After"), computeBackoffWithJitter(1));
                throw new RateLimitException("HTTP 429 rate limited by RDW API", wait);
            }
            if (status >= 500) {
                throw new ConnectorException("UPSTREAM_5XX", "HTTP " + status + " from RDW API", true);
            }
            throw new ConnectorException("UPSTREAM_4XX", "Non-retryable HTTP " + status + " from RDW API", false);
        } catch (IOException e) {
            throw new ConnectorException("NETWORK_IO", "Network I/O error calling RDW API", true, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectorException("INTERRUPTED", "Interrupted while calling RDW API", false, e);
        }
    }

    private Duration parseRetryAfter(Optional<String> headerValue, Duration fallback) {
        if (headerValue.isEmpty()) {
            return fallback;
        }
        try {
            long seconds = Long.parseLong(headerValue.get().trim());
            return Duration.ofSeconds(Math.max(0L, seconds));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private Duration computeBackoffWithJitter(int attempt) {
        long baseMillis = config.initialBackoff().toMillis() * (1L << Math.min(attempt - 1, 5));
        long jitterMillis = ThreadLocalRandom.current().nextLong(10L, Math.max(20L, baseMillis / 2));
        return Duration.ofMillis(baseMillis + jitterMillis);
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectorException("INTERRUPTED", "Interrupted during retry backoff", false, e);
        }
    }

    private List<Map<String, Object>> parseJsonRows(String body) {
        try {
            return objectMapper.readValue(body, LIST_OF_MAP);
        } catch (Exception e) {
            throw new ConnectorException("MALFORMED_PAYLOAD", "Invalid JSON returned by RDW endpoint", false, e);
        }
    }

    private static final class RateLimitException extends RuntimeException {
        private final Duration retryAfter;

        private RateLimitException(String message, Duration retryAfter) {
            super(message);
            this.retryAfter = retryAfter;
        }

        private Duration retryAfter() {
            return retryAfter;
        }
    }
}
