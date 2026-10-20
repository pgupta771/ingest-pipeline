package dev.ingest.sink.postgres;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ingest.core.ColumnSpec;
import dev.ingest.core.ColumnType;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.core.SchemaDescriptor;
import dev.ingest.core.SourceConnector;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Manages target PostgreSQL table creation and online schema evolution (`stream_schema` & `schema_events`).
 */
@Component
public class SchemaManager {

    private static final Logger log = LoggerFactory.getLogger(SchemaManager.class);
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]{0,62}$");
    private static final TypeReference<List<ColumnSpec>> COLUMN_LIST_TYPE = new TypeReference<>() {};

    private final JdbcTemplate jdbcTemplate;
    private final ConnectorRegistry connectorRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Map<String, ColumnType>> schemaCache = new ConcurrentHashMap<>();

    public SchemaManager(JdbcTemplate jdbcTemplate, ConnectorRegistry connectorRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.connectorRegistry = connectorRegistry;
    }

    /**
     * Ensures the physical PostgreSQL table and `stream_schema` metadata row exist for `stream`.
     */
    public synchronized Map<String, ColumnType> ensureStreamTableInitialized(String stream) {
        Map<String, ColumnType> cached = schemaCache.get(stream);
        if (cached != null) {
            return cached;
        }

        validateIdentifier(stream);
        SchemaDescriptor descriptor = findDescriptor(stream);

        List<String> existingJson = jdbcTemplate.query(
                "SELECT columns_json::text FROM stream_schema WHERE stream = ?",
                (rs, rowNum) -> rs.getString(1),
                stream);

        Map<String, ColumnType> columns = new LinkedHashMap<>();
        if (existingJson.isEmpty()) {
            for (ColumnSpec col : descriptor.columns()) {
                validateIdentifier(col.name());
                columns.put(col.name().toLowerCase(), col.type());
            }
            createTableIfAbsent(stream, columns);
            persistSchemaMetadata(stream, 1, columns);
        } else {
            try {
                List<ColumnSpec> specs = objectMapper.readValue(existingJson.getFirst(), COLUMN_LIST_TYPE);
                for (ColumnSpec spec : specs) {
                    columns.put(spec.name().toLowerCase(), spec.type());
                }
                createTableIfAbsent(stream, columns);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to parse stored schema for " + stream, e);
            }
        }

        schemaCache.put(stream, new LinkedHashMap<>(columns));
        return schemaCache.get(stream);
    }

    /**
     * Evaluates a single field value against the current column type and returns a {@link SchemaDecision}.
     */
    public SchemaDecision evaluateField(
            Map<String, ColumnType> currentColumns,
            String rawFieldName,
            Object value) {
        if (value == null) {
            return new SchemaDecision.NoChange();
        }
        String colName = rawFieldName.toLowerCase();
        if (!SAFE_IDENTIFIER.matcher(colName).matches()) {
            return new SchemaDecision.SendToDlq(
                    rawFieldName,
                    "INVALID_COLUMN_IDENTIFIER",
                    "Unsafe column identifier: " + rawFieldName);
        }

        ColumnType inferred = inferType(value);
        ColumnType existing = currentColumns.get(colName);
        if (existing == null) {
            return new SchemaDecision.AddColumn(colName, inferred);
        }
        if (existing == inferred || existing == ColumnType.TEXT && inferred != ColumnType.JSONB) {
            return new SchemaDecision.NoChange();
        }
        if (existing.canWidenTo(inferred)) {
            return new SchemaDecision.WidenColumn(colName, existing, inferred);
        }
        if (inferred.canWidenTo(existing)) {
            return new SchemaDecision.NoChange();
        }
        return new SchemaDecision.SendToDlq(
                colName,
                "INCOMPATIBLE_SCHEMA_TYPE",
                "Cannot convert column '" + colName + "' from " + existing + " to " + inferred);
    }

    /**
     * Applies an {@link SchemaDecision.AddColumn} or {@link SchemaDecision.WidenColumn} DDL change.
     */
    public synchronized void applySchemaChange(String stream, SchemaDecision decision) {
        validateIdentifier(stream);
        Map<String, ColumnType> columns = new LinkedHashMap<>(ensureStreamTableInitialized(stream));

        switch (decision) {
            case SchemaDecision.AddColumn add -> {
                if (columns.containsKey(add.columnName())) {
                    return;
                }
                jdbcTemplate.execute("SET LOCAL lock_timeout = '3s'");
                jdbcTemplate.execute(
                        "ALTER TABLE " + stream + " ADD COLUMN IF NOT EXISTS "
                                + add.columnName() + " " + toPostgresType(add.columnType()) + " NULL");
                columns.put(add.columnName(), add.columnType());
                int newVersion = bumpSchemaVersion(stream, columns, "COLUMN_ADDED", add.columnName(),
                        Map.of("type", add.columnType().name()));
                schemaCache.put(stream, columns);
                log.info("Added column {}.{} ({}) at schema version {}",
                        stream, add.columnName(), add.columnType(), newVersion);
            }
            case SchemaDecision.WidenColumn widen -> {
                ColumnType current = columns.get(widen.columnName());
                if (current == widen.newType()) {
                    return;
                }
                jdbcTemplate.execute("SET LOCAL lock_timeout = '3s'");
                jdbcTemplate.execute(
                        "ALTER TABLE " + stream + " ALTER COLUMN " + widen.columnName()
                                + " TYPE " + toPostgresType(widen.newType()));
                columns.put(widen.columnName(), widen.newType());
                int newVersion = bumpSchemaVersion(stream, columns, "COLUMN_WIDENED", widen.columnName(),
                        Map.of("oldType", widen.oldType().name(), "newType", widen.newType().name()));
                schemaCache.put(stream, columns);
                log.info("Widened column {}.{} from {} to {} at schema version {}",
                        stream, widen.columnName(), widen.oldType(), widen.newType(), newVersion);
            }
            default -> {
            }
        }
    }

    public static ColumnType inferType(Object value) {
        if (value instanceof Boolean) {
            return ColumnType.BOOLEAN;
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ColumnType.INTEGER;
        }
        if (value instanceof Long) {
            return ColumnType.BIGINT;
        }
        if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
            return ColumnType.NUMERIC;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            return ColumnType.JSONB;
        }
        return ColumnType.TEXT;
    }

    public static String toPostgresType(ColumnType type) {
        return switch (type) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INTEGER";
            case BIGINT -> "BIGINT";
            case NUMERIC -> "NUMERIC";
            case TIMESTAMPTZ -> "TIMESTAMPTZ";
            case TEXT -> "TEXT";
            case JSONB -> "JSONB";
        };
    }

    private void createTableIfAbsent(String stream, Map<String, ColumnType> columns) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(stream).append(" (")
                .append("pk TEXT PRIMARY KEY, ")
                .append("source_hash CHAR(64) NOT NULL, ")
                .append("extracted_at TIMESTAMPTZ NOT NULL, ")
                .append("loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()");
        for (Map.Entry<String, ColumnType> entry : columns.entrySet()) {
            ddl.append(", ").append(entry.getKey()).append(" ").append(toPostgresType(entry.getValue()));
        }
        ddl.append(")");
        jdbcTemplate.execute(ddl.toString());
    }

    private void persistSchemaMetadata(String stream, int version, Map<String, ColumnType> columns) {
        try {
            List<ColumnSpec> specs = new ArrayList<>(columns.size());
            columns.forEach((name, type) -> specs.add(new ColumnSpec(name, type, true)));
            String json = objectMapper.writeValueAsString(specs);
            jdbcTemplate.update(
                    """
                    INSERT INTO stream_schema (stream, version, columns_json, updated_at)
                    VALUES (?, ?, ?::jsonb, NOW())
                    ON CONFLICT (stream) DO UPDATE SET
                        version = EXCLUDED.version,
                        columns_json = EXCLUDED.columns_json,
                        updated_at = NOW()
                    """,
                    stream,
                    version,
                    json);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to persist stream_schema for " + stream, e);
        }
    }

    private int bumpSchemaVersion(
            String stream,
            Map<String, ColumnType> columns,
            String eventType,
            String columnName,
            Map<String, String> details) {
        Integer oldVersion = jdbcTemplate.queryForObject(
                "SELECT version FROM stream_schema WHERE stream = ?",
                Integer.class,
                stream);
        int nextVersion = (oldVersion != null ? oldVersion : 0) + 1;
        persistSchemaMetadata(stream, nextVersion, columns);
        try {
            String detailsJson = objectMapper.writeValueAsString(details);
            jdbcTemplate.update(
                    """
                    INSERT INTO schema_events (stream, old_version, new_version, event_type, column_name, details_json)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb)
                    """,
                    stream,
                    oldVersion,
                    nextVersion,
                    eventType,
                    columnName,
                    detailsJson);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write schema_events row", e);
        }
        return nextVersion;
    }

    private SchemaDescriptor findDescriptor(String stream) {
        for (SourceConnector connector : connectorRegistry.listAll()) {
            for (var sd : connector.discoverStreams()) {
                if (sd.name().equals(stream)) {
                    return sd.schema();
                }
            }
        }
        return new SchemaDescriptor(stream, "pk", List.of());
    }

    private static void validateIdentifier(String identifier) {
        if (identifier == null || !SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + identifier);
        }
    }
}
