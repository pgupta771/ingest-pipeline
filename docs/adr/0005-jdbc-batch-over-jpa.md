# ADR-0005: JDBC Batch Writes (`reWriteBatchedInserts=true`) Over JPA on the Hot Path

## Status
Accepted

## Context
JPA/Hibernate tracks entity state in a first-level persistence context, requires compile-time entity mappings that cannot adapt to runtime `ALTER TABLE` schema drift, and incurs per-row reflection and dirty-checking overhead.

## Decision
We use Spring `JdbcTemplate` batch updates paired with the PostgreSQL driver flag `reWriteBatchedInserts=true`, which folds batched prepared statements into multi-row bulk `INSERT ... VALUES (...), (...) ON CONFLICT` network packets.

## Consequences
Dynamic columns added via online schema evolution are immediately writable without class reloading, heap allocation remains flat during 200,000-row loads, and write throughput increases by an order of magnitude compared to entity-by-entity ORM persistence.
