# `engine` — Extraction Orchestrator & Checkpointing

Coordinates paginated source reads, publishes batches to `ingest.raw.<stream>` with an idempotent Kafka producer (`acks=all`), and commits the continuation cursor to PostgreSQL `extract_checkpoint` strictly after all broker acknowledgments arrive.
