# `sink-postgres` — Transactional Offset & Idempotent Upsert Loader

Consumes batches from Kafka with `enable.auto.commit=false`, seeks partitions from PostgreSQL `consumer_offsets` via `ConsumerSeekAware`, deduplicates keys within each batch, and atomically executes JDBC batch upserts (`WHERE target.source_hash IS DISTINCT FROM EXCLUDED.source_hash`), dead-letter inserts, and offset updates in a single PostgreSQL transaction.
