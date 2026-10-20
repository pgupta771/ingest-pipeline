package dev.ingest.sink.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.ingest.core.SourceRecord;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

/**
 * Batch Kafka consumer that seeks offsets from PostgreSQL {@code consumer_offsets} on partition assignment
 * and delegates batch persistence to {@link PostgresTransactionalWriter}.
 */
@Component
public class KafkaBatchSinkConsumer implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchSinkConsumer.class);
    private static final String TOPIC_PREFIX = "ingest.raw.";

    private final PostgresTransactionalWriter writer;
    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public KafkaBatchSinkConsumer(
            PostgresTransactionalWriter writer,
            JdbcTemplate jdbcTemplate,
            MeterRegistry meterRegistry) {
        this.writer = writer;
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public void onPartitionsAssigned(
            Map<TopicPartition, Long> assignments,
            ConsumerSeekCallback callback) {
        for (TopicPartition tp : assignments.keySet()) {
            if (!tp.topic().startsWith(TOPIC_PREFIX)) {
                continue;
            }
            List<Long> stored = jdbcTemplate.query(
                    "SELECT next_offset FROM consumer_offsets WHERE topic = ? AND partition = ?",
                    (rs, rowNum) -> rs.getLong("next_offset"),
                    tp.topic(),
                    tp.partition());
            if (!stored.isEmpty()) {
                long nextOffset = stored.getFirst();
                log.info("Seeking topic={} partition={} to PostgreSQL stored next_offset={}",
                        tp.topic(), tp.partition(), nextOffset);
                callback.seek(tp.topic(), tp.partition(), nextOffset);
            } else {
                log.info("No stored offset in PostgreSQL for topic={} partition={}; seeking to beginning",
                        tp.topic(), tp.partition());
                callback.seekToBeginning(tp.topic(), tp.partition());
            }
        }
    }

    @KafkaListener(
            id = "postgres-sink-batch-listener",
            topicPattern = "ingest\\.raw\\..*",
            containerFactory = "batchKafkaListenerContainerFactory")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        Map<String, List<SourceRecord>> byStream = new LinkedHashMap<>();
        Map<String, List<DeadLetterEntry>> deadByStream = new LinkedHashMap<>();
        Map<String, Map<TopicPartition, Long>> offsetsByStream = new HashMap<>();

        for (ConsumerRecord<String, String> cr : records) {
            String stream = extractStreamName(cr.topic());
            TopicPartition tp = new TopicPartition(cr.topic(), cr.partition());
            offsetsByStream
                    .computeIfAbsent(stream, k -> new HashMap<>())
                    .merge(tp, cr.offset() + 1L, Math::max);

            try {
                SourceRecord parsed = objectMapper.readValue(cr.value(), SourceRecord.class);
                byStream.computeIfAbsent(stream, k -> new ArrayList<>()).add(parsed);
            } catch (Exception e) {
                deadByStream.computeIfAbsent(stream, k -> new ArrayList<>()).add(
                        new DeadLetterEntry(
                                stream,
                                cr.key(),
                                "MALFORMED_RECORD_JSON",
                                e.getMessage(),
                                cr.value() != null ? cr.value() : "",
                                cr.topic(),
                                cr.partition(),
                                cr.offset()));
            }
        }

        for (String stream : offsetsByStream.keySet()) {
            writer.writeBatchTransactionally(
                    stream,
                    byStream.getOrDefault(stream, List.of()),
                    deadByStream.getOrDefault(stream, List.of()),
                    offsetsByStream.get(stream));
        }

        sample.stop(Timer.builder("ingest_batch_duration_seconds")
                .register(meterRegistry));
    }

    private static String extractStreamName(String topic) {
        return topic.startsWith(TOPIC_PREFIX)
                ? topic.substring(TOPIC_PREFIX.length())
                : topic;
    }
}
