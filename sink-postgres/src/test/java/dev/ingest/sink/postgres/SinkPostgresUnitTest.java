package dev.ingest.sink.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ingest.core.ColumnSpec;
import dev.ingest.core.ColumnType;
import dev.ingest.core.ConnectorRegistry;
import dev.ingest.core.Cursor;
import dev.ingest.core.Page;
import dev.ingest.core.SchemaDescriptor;
import dev.ingest.core.SourceConnector;
import dev.ingest.core.SourceSession;
import dev.ingest.core.StreamDescriptor;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class SinkPostgresUnitTest {

    @Test
    void schemaManagerEvaluatesAllDriftDecisionsAndEvictsCacheOnRollback() {
        ConnectorRegistry registry = new ConnectorRegistry();
        registry.register(new SourceConnector() {
            @Override
            public String id() {
                return "rdw";
            }

            @Override
            public List<StreamDescriptor> discoverStreams() {
                return List.of(new StreamDescriptor(
                        "rdw_vehicles",
                        "desc",
                        new SchemaDescriptor("rdw_vehicles", "kenteken",
                                List.of(new ColumnSpec("kenteken", ColumnType.TEXT, false)))));
            }

            @Override
            public SchemaDescriptor describe(String stream) {
                return discoverStreams().getFirst().schema();
            }

            @Override
            public SourceSession openSession(String stream, Optional<Cursor> cursor) {
                return pageSize -> Page.empty(cursor);
            }
        });

        SchemaManager manager = new SchemaManager(new JdbcTemplate() {
            @Override
            public void execute(String sql) {
                // no-op for unit test
            }

            @Override
            public int update(String sql, Object... args) {
                return 1;
            }

            @Override
            public <T> List<T> query(String sql, org.springframework.jdbc.core.RowMapper<T> rowMapper, Object... args) {
                return List.of();
            }

            @Override
            public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) {
                return requiredType.cast(1);
            }
        }, registry);

        TransactionSynchronizationManager.initSynchronization();
        try {
            Map<String, ColumnType> cols = manager.ensureStreamTableInitialized("rdw_vehicles");
            assertThat(cols).containsEntry("kenteken", ColumnType.TEXT);

            manager.applySchemaChange("rdw_vehicles", new SchemaDecision.AddColumn("extra_col", ColumnType.INTEGER));
            assertThat(manager.ensureStreamTableInitialized("rdw_vehicles"))
                    .containsEntry("extra_col", ColumnType.INTEGER);

            // Simulate transaction rollback -> must evict cached 'extra_col'
            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
            }

            Map<String, ColumnType> afterRollback = manager.ensureStreamTableInitialized("rdw_vehicles");
            assertThat(afterRollback).doesNotContainKey("extra_col");
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }

        Map<String, ColumnType> current = new LinkedHashMap<>();
        current.put("kenteken", ColumnType.TEXT);
        current.put("massa_ledig_voertuig", ColumnType.INTEGER);

        assertThat(manager.evaluateField(current, "massa_ledig_voertuig", null))
                .isInstanceOf(SchemaDecision.NoChange.class);
        assertThat(manager.evaluateField(current, "new_col", "hello"))
                .isEqualTo(new SchemaDecision.AddColumn("new_col", ColumnType.TEXT));
        assertThat(manager.evaluateField(current, "massa_ledig_voertuig", 5_000_000_000L))
                .isEqualTo(new SchemaDecision.WidenColumn("massa_ledig_voertuig", ColumnType.INTEGER, ColumnType.BIGINT));
        assertThat(manager.evaluateField(current, "massa_ledig_voertuig", "invalid-number"))
                .isInstanceOf(SchemaDecision.SendToDlq.class);
        assertThat(manager.evaluateField(current, "bad-col-name!", 123))
                .isInstanceOf(SchemaDecision.SendToDlq.class);
        assertThatThrownBy(() -> manager.ensureStreamTableInitialized("bad-table!"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(SchemaManager.inferType(true)).isEqualTo(ColumnType.BOOLEAN);
        assertThat(SchemaManager.inferType(42)).isEqualTo(ColumnType.INTEGER);
        assertThat(SchemaManager.inferType(42L)).isEqualTo(ColumnType.BIGINT);
        assertThat(SchemaManager.inferType(new BigDecimal("12.34"))).isEqualTo(ColumnType.NUMERIC);
        assertThat(SchemaManager.inferType(List.of("a"))).isEqualTo(ColumnType.JSONB);
        assertThat(SchemaManager.toPostgresType(ColumnType.TIMESTAMPTZ)).isEqualTo("TIMESTAMPTZ");
    }
}
