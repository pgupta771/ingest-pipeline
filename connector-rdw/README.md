# `connector-rdw` — Dutch RDW Open Data Connector

Extracts registered vehicle records (`m9d7-ebf2`) from the Dutch RDW Socrata SODA API using keyset pagination (`$order=kenteken ASC`, `$where=kenteken > ':lastKey'`), Resilience4j circuit breaking, and `Retry-After` rate-limit backoff.
