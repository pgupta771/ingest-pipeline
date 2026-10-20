# ADR-0001: Store Kafka Consumer Offsets Transactionally in PostgreSQL

## Status
Accepted

## Context
Relying on Kafka consumer-group offset commits (`__consumer_offsets`) alongside a relational sink introduces a dual-write window: if the application commits rows to PostgreSQL and crashes before committing offsets to Kafka (or vice versa), records are either reprocessed without coordination or silently lost.

## Decision
We disable Kafka auto-commit (`enable.auto.commit=false`) and store `(topic, partition, next_offset)` inside PostgreSQL (`consumer_offsets`) within the exact same ACID transaction that upserts the target stream rows and `dead_letters`. Whenever partitions are assigned or rebalanced (`ConsumerSeekAware.onPartitionsAssigned`), the consumer queries `consumer_offsets` and explicitly calls `seek(topic, partition, next_offset)`.

## Consequences
Row mutations and offset progression commit or roll back atomically in PostgreSQL, eliminating cross-system two-phase commit overhead while guaranteeing effectively-once sink semantics.
