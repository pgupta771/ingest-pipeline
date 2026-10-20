package dev.ingest.connector.ecb;

import dev.ingest.core.ColumnSpec;
import dev.ingest.core.ColumnType;
import dev.ingest.core.ConnectorException;
import dev.ingest.core.Cursor;
import dev.ingest.core.Page;
import dev.ingest.core.SchemaDescriptor;
import dev.ingest.core.SourceConnector;
import dev.ingest.core.SourceRecord;
import dev.ingest.core.SourceSession;
import dev.ingest.core.StreamDescriptor;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Streaming StAX connector for European Central Bank (ECB) foreign exchange reference rate XML files.
 * Maintains an open {@link XMLStreamReader} across pages so arbitrarily large XML files
 * are ingested in constant O(1) heap memory.
 */
public class EcbXmlConnector implements SourceConnector {

    public static final String CONNECTOR_ID = "ecb-xml";
    public static final String STREAM_NAME = "ecb_exchange_rates";
    public static final String PRIMARY_KEY = "rate_key";

    private final Path xmlFilePath;
    private final String sourceFileName;
    private final XMLInputFactory xmlInputFactory;

    public EcbXmlConnector(Path xmlFilePath) {
        this.xmlFilePath = Objects.requireNonNull(xmlFilePath, "xmlFilePath must not be null");
        this.sourceFileName = xmlFilePath.getFileName().toString();
        this.xmlInputFactory = XMLInputFactory.newFactory();
        this.xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        this.xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    }

    @Override
    public String id() {
        return CONNECTOR_ID;
    }

    @Override
    public List<StreamDescriptor> discoverStreams() {
        return List.of(new StreamDescriptor(
                STREAM_NAME,
                "ECB Euro foreign exchange reference rates (XML)",
                describe(STREAM_NAME)));
    }

    @Override
    public SchemaDescriptor describe(String stream) {
        if (!STREAM_NAME.equals(stream)) {
            throw new IllegalArgumentException("Unsupported stream for ECB XML connector: " + stream);
        }
        return new SchemaDescriptor(
                STREAM_NAME,
                PRIMARY_KEY,
                List.of(
                        new ColumnSpec("rate_key", ColumnType.TEXT, false),
                        new ColumnSpec("rate_date", ColumnType.TEXT, false),
                        new ColumnSpec("currency", ColumnType.TEXT, false),
                        new ColumnSpec("rate", ColumnType.NUMERIC, false)));
    }

    @Override
    public SourceSession openSession(String stream, Optional<Cursor> cursor) {
        if (!STREAM_NAME.equals(stream)) {
            throw new IllegalArgumentException("Unsupported stream for ECB XML connector: " + stream);
        }
        long skipRecords = cursor.map(this::parseCursorOffset).orElse(0L);
        return new StaxEcbSourceSession(skipRecords);
    }

    private long parseCursorOffset(Cursor cursor) {
        String raw = cursor.value();
        int idx = raw.lastIndexOf(':');
        if (idx <= 0 || idx == raw.length() - 1) {
            throw new IllegalArgumentException("Invalid ECB XML cursor format (expected <file>:<offset>): " + raw);
        }
        return Long.parseLong(raw.substring(idx + 1));
    }

    private final class StaxEcbSourceSession implements SourceSession {

        private final InputStream inputStream;
        private final XMLStreamReader reader;
        private final MessageDigest sha256;
        private String currentDate;
        private long recordCounter;
        private SourceRecord lookahead;
        private boolean initialized;

        private StaxEcbSourceSession(long skipRecords) {
            try {
                this.inputStream = new BufferedInputStream(Files.newInputStream(xmlFilePath), 64 * 1024);
                this.reader = xmlInputFactory.createXMLStreamReader(inputStream, StandardCharsets.UTF_8.name());
                this.sha256 = MessageDigest.getInstance("SHA-256");
                this.recordCounter = 0L;
                if (skipRecords > 0) {
                    fastForward(skipRecords);
                }
            } catch (IOException | XMLStreamException | NoSuchAlgorithmException e) {
                throw new ConnectorException("XML_OPEN_ERROR", "Failed to open ECB XML stream", false, e);
            }
        }

        private void fastForward(long targetOffset) throws XMLStreamException {
            while (recordCounter < targetOffset && reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT && "Cube".equals(reader.getLocalName())) {
                    String timeAttr = reader.getAttributeValue(null, "time");
                    if (timeAttr != null) {
                        currentDate = timeAttr;
                    } else if (reader.getAttributeValue(null, "currency") != null) {
                        recordCounter++;
                    }
                }
            }
        }

        @Override
        public Page nextPage(int pageSize) {
            if (pageSize <= 0) {
                throw new IllegalArgumentException("pageSize must be > 0");
            }
            try {
                if (!initialized) {
                    initialized = true;
                    lookahead = readNextRateRecord(Instant.now());
                }
            } catch (XMLStreamException | NumberFormatException e) {
                throw new ConnectorException("MALFORMED_XML", "Failed to parse ECB XML element", false, e);
            }
            if (lookahead == null) {
                return Page.empty(currentCursor());
            }

            List<SourceRecord> records = new ArrayList<>(Math.min(pageSize, 4096));
            Instant now = Instant.now();

            try {
                while (records.size() < pageSize && lookahead != null) {
                    records.add(lookahead);
                    recordCounter++;
                    lookahead = readNextRateRecord(now);
                }
                return new Page(records, currentCursor(), lookahead != null);
            } catch (XMLStreamException | NumberFormatException e) {
                throw new ConnectorException("MALFORMED_XML", "Failed to parse ECB XML element", false, e);
            }
        }

        private SourceRecord readNextRateRecord(Instant extractedAt) throws XMLStreamException {
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT && "Cube".equals(reader.getLocalName())) {
                    String timeAttr = reader.getAttributeValue(null, "time");
                    if (timeAttr != null) {
                        currentDate = timeAttr;
                        continue;
                    }
                    String currency = reader.getAttributeValue(null, "currency");
                    String rateStr = reader.getAttributeValue(null, "rate");
                    if (currency != null && rateStr != null && currentDate != null) {
                        BigDecimal rate = new BigDecimal(rateStr);
                        String pk = currentDate + "_" + currency;

                        Map<String, Object> payload = new LinkedHashMap<>(4);
                        payload.put("rate_key", pk);
                        payload.put("rate_date", currentDate);
                        payload.put("currency", currency);
                        payload.put("rate", rate);

                        String hash = fastCanonicalHash(pk, currentDate, currency, rateStr);
                        return new SourceRecord(STREAM_NAME, pk, payload, extractedAt, hash);
                    }
                }
            }
            return null;
        }

        private Optional<Cursor> currentCursor() {
            return recordCounter > 0
                    ? Optional.of(Cursor.of(sourceFileName + ":" + recordCounter))
                    : Optional.empty();
        }

        private String fastCanonicalHash(String pk, String date, String currency, String rateStr) {
            sha256.reset();
            String canonical = "{\"currency\":\"" + currency
                    + "\",\"rate\":" + rateStr
                    + ",\"rate_date\":\"" + date
                    + "\",\"rate_key\":\"" + pk + "\"}";
            return HexFormat.of().formatHex(sha256.digest(canonical.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (XMLStreamException ignored) {
            }
            try {
                inputStream.close();
            } catch (IOException ignored) {
            }
        }
    }
}
