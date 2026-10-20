# `connector-ecb-xml` — Streaming StAX ECB Foreign Exchange XML Connector

Parses European Central Bank (`gesmes:Envelope` / `Cube`) XML reference rate feeds in $O(1)$ constant heap using a stateful StAX `XMLStreamReader` session, supporting mid-file crash recovery via `<fileName>:<recordCount>` cursors and streaming 150 MB+ XML files comfortably within `-Xmx64m`.
