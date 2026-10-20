# ADR-0003: Idempotent Upsert Keyed on Primary Key and Canonical Source Hash

## Status
Accepted

## Context
At-least-once extraction and crash recovery inevitably replay previously seen records. Unconditional `ON CONFLICT (pk) DO UPDATE` statements rewrite unchanged tuples, bloat PostgreSQL MVCC dead tuples (requiring aggressive `VACUUM`), and overwrite `loaded_at` audit timestamps.

## Decision
Every extracted `SourceRecord` computes a canonical SHA-256 `source_hash` over its sorted key-value payload. Before executing a JDBC batch, the loader deduplicates records sharing the same `pk` in memory (preventing PostgreSQL `SQLSTATE 21000` within a single statement) and executes `INSERT ... ON CONFLICT (pk) DO UPDATE SET ... WHERE target.source_hash IS DISTINCT FROM EXCLUDED.source_hash`.

## Consequences
Replaying 200,000 identical records performs zero physical heap updates after index lookup, preserving `loaded_at` timestamps and keeping table bloat near zero.
