package dev.ingest.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CoreSpiTest {

    @Test
    void payloadHasherProducesIdenticalDigestRegardlessOfKeyOrder() {
        Map<String, Object> m1 = new LinkedHashMap<>();
        m1.put("kenteken", "AB123C");
        m1.put("merk", "VOLVO");
        m1.put("bouwjaar", 2022);

        Map<String, Object> m2 = new LinkedHashMap<>();
        m2.put("bouwjaar", 2022);
        m2.put("kenteken", "AB123C");
        m2.put("merk", "VOLVO");

        SourceRecord r1 = SourceRecord.create("rdw_vehicles", "AB123C", m1, Instant.now());
        SourceRecord r2 = SourceRecord.create("rdw_vehicles", "AB123C", m2, Instant.now());

        assertThat(r1.sourceHash()).isEqualTo(r2.sourceHash()).hasSize(64);
    }

    @Test
    void columnTypeWideningFollowsSafeRules() {
        assertThat(ColumnType.INTEGER.canWidenTo(ColumnType.BIGINT)).isTrue();
        assertThat(ColumnType.INTEGER.canWidenTo(ColumnType.NUMERIC)).isTrue();
        assertThat(ColumnType.BIGINT.canWidenTo(ColumnType.NUMERIC)).isTrue();
        assertThat(ColumnType.BIGINT.canWidenTo(ColumnType.INTEGER)).isFalse();
        assertThat(ColumnType.TEXT.canWidenTo(ColumnType.INTEGER)).isFalse();
    }

    @Test
    void cursorAndRegistryValidateInputs() {
        assertThatThrownBy(() -> Cursor.of("   ")).isInstanceOf(IllegalArgumentException.class);

        ConnectorRegistry registry = new ConnectorRegistry();
        SourceConnector dummy = new SourceConnector() {
            @Override
            public String id() {
                return "dummy";
            }

            @Override
            public List<StreamDescriptor> discoverStreams() {
                return List.of();
            }

            @Override
            public SchemaDescriptor describe(String stream) {
                return new SchemaDescriptor(stream, "id", List.of(new ColumnSpec("id", ColumnType.TEXT, false)));
            }

            @Override
            public SourceSession openSession(String stream, Optional<Cursor> cursor) {
                return pageSize -> Page.empty(cursor);
            }
        };

        registry.register(dummy);
        assertThat(registry.getRequired("dummy")).isSameAs(dummy);
        assertThat(registry.listAll()).hasSize(1);
        assertThatThrownBy(() -> registry.getRequired("missing"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
