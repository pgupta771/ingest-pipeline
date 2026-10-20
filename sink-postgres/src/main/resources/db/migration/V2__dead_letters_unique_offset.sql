CREATE UNIQUE INDEX IF NOT EXISTS uq_dead_letters_topic_partition_offset
    ON dead_letters (kafka_topic, kafka_partition, kafka_offset);
