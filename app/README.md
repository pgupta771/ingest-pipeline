# `app` — Spring Boot Runner, REST API & Deterministic Fixture Server

Exposes minimal REST endpoints (`/api/v1/syncs`, `/api/v1/streams`), Prometheus metrics (`/actuator/prometheus`), and an optional deterministic 200,000-record RDW fixture server for offline verification and crash-recovery testing.
