# ADR-0002: Keyset Pagination vs. Offset Pagination for Socrata SODA Extraction

## Status
Accepted

## Context
The Dutch RDW Open Data portal (`opendata.rdw.nl`, dataset `m9d7-ebf2`) exposes over 15 million vehicle registrations via the Socrata SODA API. Paginating deep into a multi-million-row dataset with `$limit` and `$offset` forces the upstream database to scan and discard `$offset` rows on every request ($O(N^2)$ cumulative cost), hits Socrata's high-offset throttling limits, and silently skips or duplicates records whenever rows are inserted or deleted between page requests.

## Decision
We paginate strictly by keyset (`$order=kenteken ASC` and `$where=kenteken > ':lastKenteken'`) and store the highest seen `kenteken` value as our opaque `Cursor`. Additionally, `RdwVehicleConnector` enforces monotonic key progression across pages so any duplicate boundary record is filtered deterministically.

## Consequences
Each page request executes an indexed range scan in constant time regardless of depth, checkpoints are compact (`kenteken` string), and crash recovery resumes from the exact last committed license plate key.
