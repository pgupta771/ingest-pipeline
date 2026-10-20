package dev.ingest.sink.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ingest.core.ColumnType;
import dev.ingest.core.SourceRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Executes schema evolution, in-batch deduplicated upserts, dead-letter persistence,
 * sync_runs counter updates, and Kafka consumer offset commits inside a single PostgreSQL transaction.
 */
@Service
public class PostgresTransactionalWriter {

    public static final String DLQ_TOPIC = "ingest.dlq";

    private final JdbcTemplate jdbcTemplate;
    private final SchemaManager schemaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PostgresTransactionalWriter(
            JdbcTemplate jdbcTemplate,
            SchemaManager schemaManager,
            KafkaTemplate<String, String> kafkaTemplate,
            MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaManager = schemaManager;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Atomically processes a batch of consumed record envelopes for a stream and commits the Kafka offsets.
     */
    @Transactional
    public void writeBatchTransactionally(
            String stream,
            List<ConsumedRecordEnvelope> rawEnvelopes,
            List<DeadLetterEntry> preParsedDeadLetters,
            Map<TopicPartition, Long> nextOffsets) {

        Map<String, ColumnType> schemaColumns = schemaManager.ensureStreamTableInitialized(stream);

        List<DeadLetterEntry> allDeadLetters = new ArrayList<>(preParsedDeadLetters);
        Map<String, SourceRecord> deduplicatedByPk = new LinkedHashMap<>();

        for (ConsumedRecordEnvelope env : rawEnvelopes) {
            SourceRecord record = env.record();
            SchemaDecision.SendToDlq rejection = null;
            List<SchemaDecision> pendingEvolutions = new ArrayList<>();

            // Pass 1: Validate all fields first so a rejected record never alters the table schema
            for (Map.Entry<String, Object> field : record.payload().entrySet()) {
                SchemaDecision decision = schemaManager.evaluateField(
                        schemaColumns, field.getKey(), field.getValue());
                if (decision instanceof SchemaDecision.SendToDlq dlq) {
                    rejection = dlq;
                    break;
                } else if (decision instanceof SchemaDecision.AddColumn
                        || decision instanceof SchemaDecision.WidenColumn) {
                    pendingEvolutions.add(decision);
                }
            }

            if (rejection != null) {
                allDeadLetters.add(new DeadLetterEntry(
                        stream,
                        record.primaryKey(),
                        rejection.reasonCode(),
                        rejection.reasonDetail(),
                        toJsonSafe(record),
                        env.kafkaTopic(),
                        env.kafkaPartition(),
                        env.kafkaOffset()));
            } else {
                // Pass 2: Apply safe schema evolutions for valid record
                for (SchemaDecision evolution : pendingEvolutions) {
                    schemaManager.applySchemaChange(stream, evolution);
                    schemaColumns = schemaManager.ensureStreamTableInitialized(stream);
                }
                deduplicatedByPk.put(record.primaryKey(), record);
            }
        }

        List<SourceRecord> validRecords = List.copyOf(deduplicatedByPk.values());
        if (!validRecords.isEmpty()) {
            batchUpsertRecords(stream, new ArrayList<>(schemaColumns.entrySet()), validRecords);
            Counter.builder("ingest_records_loaded_total")
                    .tag("stream", stream)
                    .register(meterRegistry)
                    .increment(validRecords.size());
        }

        if (!allDeadLetters.isEmpty()) {
            batchInsertDeadLetters(allDeadLetters);
            publishDeadLettersAfterCommit(allDeadLetters);
            Counter.builder("ingest_records_dead_total")
                    .tag("stream", stream)
                    .register(meterRegistry)
                    .increment(allDeadLetters.size());
        }

        updateSyncRunCounters(stream, validRecords.size(), allDeadLetters.size());
        upsertConsumerOffsets(nextOffsets);
    }

    private void batchUpsertRecords(
            String stream,
            List<Map.Entry<String, ColumnType>> orderedColumns,
            List<SourceRecord> records) {

        List<String> colNames = orderedColumns.stream().map(Map.Entry::getKey).toList();
        String insertCols = colNames.isEmpty() ? "" : ", " + String.join(", ", colNames);
        String placeholders = colNames.isEmpty()
                ? ""
                : ", " + colNames.stream().map(c -> "?").collect(Collectors.joining(", "));
        String updateAssignments = colNames.isEmpty()
                ? ""
                : ", " + colNames.stream()
                        .map(c -> c + " = EXCLUDED." + c)
                        .collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + stream + " (pk, source_hash, extracted_at, loaded_at" + insertCols + ") "
                + "VALUES (?, ?, ?, NOW()" + placeholders + ") "
                + "ON CONFLICT (pk) DO UPDATE SET "
                + "source_hash = EXCLUDED.source_hash, "
                + "extracted_at = EXCLUDED.extracted_at, "
                + "loaded_at = NOW()"
                + updateAssignments
                + " WHERE " + stream + ".source_hash IS DISTINCT FROM EXCLUDED.source_hash";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws java.sql.SQLException {
                SourceRecord rec = records.get(i);
                ps.setString(1, rec.primaryKey());
                ps.setString(2, rec.sourceHash());
                ps.setTimestamp(3, Timestamp.from(rec.extractedAt()));

                int paramIdx = 4;
                for (Map.Entry<String, ColumnType> col : orderedColumns) {
                    Object rawVal = findCaseInsensitive(rec.payload(), col.getKey());
                    bindParameter(ps, paramIdx++, rawVal, col.getValue());
                }
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    private Object findCaseInsensitive(Map<String, Object> payload, String lowerColumn) {
        Object direct = payload.get(lowerColumn);
        if (direct != null) {
            return direct;
        }
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(lowerColumn)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private void bindParameter(PreparedStatement ps, int idx, Object value, ColumnType type)
            throws java.sql.SQLException {
        if (value == null) {
            ps.setNull(idx, Types.NULL);
            return;
        }
        switch (type) {
            case BOOLEAN -> ps.setBoolean(idx, Boolean.parseBoolean(value.toString()));
            case INTEGER -> ps.setInt(idx, ((Number) value).intValue());
            case BIGINT -> ps.setLong(idx, ((Number) value).longValue());
            case NUMERIC -> ps.setBigDecimal(idx, new BigDecimal(value.toString()));
            case JSONB -> ps.setObject(idx, toJsonSafe(value), Types.OTHER);
            default -> ps.setString(idx, value.toString());
        }
    }

    private void batchInsertDeadLetters(List<DeadLetterEntry> entries) {
        String sql = """
                INSERT INTO dead_letters
                    (stream, pk, reason_code, reason_detail, raw_payload, kafka_topic, kafka_partition, kafka_offset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
                """;
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws java.sql.SQLException {
                DeadLetterEntry d = entries.get(i);
                ps.setString(1, d.stream());
                ps.setString(2, d.primaryKey());
                ps.setString(3, d.reasonCode());
                ps.setString(4, d.reasonDetail());
                ps.setString(5, d.rawPayload());
                ps.setString(6, d.kafkaTopic());
                ps.setInt(7, d.kafkaPartition());
                ps.setLong(8, d.kafkaOffset());
            }

            @Override
            public int getBatchSize() {
                return entries.size();
            }
        });
    }

    private void publishDeadLettersAfterCommit(List<DeadLetterEntry> entries) {
        Runnable sendTask = () -> {
            List<CompletableFuture<?>> futures = new ArrayList<>(entries.size());
            for (DeadLetterEntry entry : entries) {
                String key = entry.primaryKey() != null ? entry.primaryKey() : "UNKNOWN";
                futures.add(kafkaTemplate.send(DLQ_TOPIC, key, toJsonSafe(entry)));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        };

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    sendTask.run();
                }
            });
        } else {
            sendTask.run();
        }
    }

    private void updateSyncRunCounters(String stream, int loadedDelta, int deadDelta) {
        if (loadedDelta == 0 && deadDelta == 0) {
            return;
        }
        jdbcTemplate.update(
                """
                UPDATE sync_runs
                SET records_loaded = records_loaded + ?,
                    records_dead   = records_dead + ?
                WHERE id = (
                    SELECT id FROM sync_runs
                    WHERE stream = ?
                    ORDER BY started_at DESC
                    LIMIT 1
                )
                """,
                loadedDelta,
                deadDelta,
                stream);
    }

    private void upsertConsumerOffsets(Map<TopicPartition, Long> nextOffsets) {
        for (Map.Entry<TopicPartition, Long> entry : nextOffsets.entrySet()) {
            jdbcTemplate.update(
                    """
                    INSERT INTO consumer_offsets (topic, partition, next_offset, updated_at)
                    VALUES (?, ?, ?, NOW())
                    ON CONFLICT (topic, partition) DO UPDATE SET
                        next_offset = GREATEST(consumer_offsets.next_offset, EXCLUDED.next_offset),
                        updated_at  = NOW()
                    """,
                    entry.getKey().topic(),
                    entry.getKey().partition(),
                    entry.getValue());
        }
    }

    private String toJsonSafe(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return String.valueOf(obj);
        }
    }
}
