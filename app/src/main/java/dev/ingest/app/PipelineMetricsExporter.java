package dev.ingest.app;

import dev.ingest.connector.rdw.RdwVehicleConnector;
import dev.ingest.core.ConnectorRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Registers Prometheus gauges for checkpoint age, consumer lag, and connector retry totals.
 */
@Component
public class PipelineMetricsExporter {

    private final JdbcTemplate jdbcTemplate;
    private final ConnectorRegistry connectorRegistry;

    public PipelineMetricsExporter(
            MeterRegistry meterRegistry,
            JdbcTemplate jdbcTemplate,
            ConnectorRegistry connectorRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.connectorRegistry = connectorRegistry;

        Gauge.builder("ingest_checkpoint_age_seconds", this, PipelineMetricsExporter::computeCheckpointAgeSeconds)
                .description("Age in seconds of the most recent extraction checkpoint")
                .register(meterRegistry);

        Gauge.builder("ingest_consumer_lag", this, PipelineMetricsExporter::computeConsumerLag)
                .description("Estimated unprocessed record lag between extractor and PostgreSQL loader")
                .register(meterRegistry);

        Gauge.builder("ingest_retries_total", this, PipelineMetricsExporter::computeRetriesTotal)
                .description("Total HTTP retry attempts across source connectors")
                .register(meterRegistry);
    }

    double computeCheckpointAgeSeconds() {
        try {
            Double age = jdbcTemplate.queryForObject(
                    "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - updated_at))), 0) FROM extract_checkpoint",
                    Double.class);
            return age != null ? Math.max(0.0, age) : 0.0;
        } catch (Exception e) {
            return 0.0;
        }
    }

    double computeConsumerLag() {
        try {
            Long extracted = jdbcTemplate.queryForObject(
                    "SELECT COALESCE(SUM(records_extracted), 0) FROM sync_runs", Long.class);
            Long consumed = jdbcTemplate.queryForObject(
                    "SELECT COALESCE(SUM(next_offset), 0) FROM consumer_offsets", Long.class);
            if (extracted == null || consumed == null) {
                return 0.0;
            }
            return Math.max(0L, extracted - consumed);
        } catch (Exception e) {
            return 0.0;
        }
    }

    double computeRetriesTotal() {
        return connectorRegistry.find(RdwVehicleConnector.CONNECTOR_ID)
                .filter(RdwVehicleConnector.class::isInstance)
                .map(RdwVehicleConnector.class::cast)
                .map(c -> (double) c.totalRetries())
                .orElse(0.0);
    }
}
