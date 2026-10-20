package dev.ingest.sink.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ingest.core.ColumnType;
import dev.ingest.core.ConnectorRegistry;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class SinkPostgresUnitTest {

    @Test
    void schemaManagerEvaluatesAllDriftDecisionsCorrectly() {
        SchemaManager manager = new SchemaManager(new JdbcTemplate(), new ConnectorRegistry());
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

        assertThat(SchemaManager.inferType(true)).isEqualTo(ColumnType.BOOLEAN);
        assertThat(SchemaManager.inferType(42)).isEqualTo(ColumnType.INTEGER);
        assertThat(SchemaManager.inferType(42L)).isEqualTo(ColumnType.BIGINT);
        assertThat(SchemaManager.inferType(new BigDecimal("12.34"))).isEqualTo(ColumnType.NUMERIC);
        assertThat(SchemaManager.inferType(List.of("a"))).isEqualTo(ColumnType.JSONB);
        assertThat(SchemaManager.toPostgresType(ColumnType.TIMESTAMPTZ)).isEqualTo("TIMESTAMPTZ");
    }
}
