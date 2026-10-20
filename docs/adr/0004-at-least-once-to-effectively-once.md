# ADR-0004: End-to-End Effectively-Once Semantics Over At-Least-Once Transport

## Status
Accepted

## Context
Distributed Kafka transactions across external HTTP APIs, Kafka brokers, and PostgreSQL add significant latency and operational complexity.

## Decision
We combine three composable invariants: (1) the Extractor updates `extract_checkpoint` only after all Kafka producer `acks=all` futures succeed for a page; (2) the Kafka producer enables `enable.idempotence=true`; and (3) the PostgreSQL Loader commits idempotent upserts (`source_hash`-guarded) and `consumer_offsets` in a single database transaction.

## Consequences
A crash at any point in extraction or loading results in bounded page/batch replay that converges to the exact same database state with zero duplicates and zero missing records.
