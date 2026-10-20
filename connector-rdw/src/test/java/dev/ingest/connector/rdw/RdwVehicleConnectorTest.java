package dev.ingest.connector.rdw;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import dev.ingest.core.ConnectorException;
import dev.ingest.core.Cursor;
import dev.ingest.core.Page;
import dev.ingest.core.SourceSession;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

class RdwVehicleConnectorTest {

    @RegisterExtension
    static WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    private RdwVehicleConnector connector;

    @BeforeEach
    void setUp() {
        RdwConnectorConfig config = new RdwConnectorConfig(
                URI.create(wm.baseUrl()),
                "m9d7-ebf2",
                Optional.of("test-token"),
                Duration.ofSeconds(5),
                4,
                Duration.ofMillis(30));
        connector = new RdwVehicleConnector(config);
    }

    @Test
    void handlesEmptyPage() {
        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .willReturn(aResponse().withStatus(200).withBody("[]")));

        try (SourceSession session = connector.openSession(RdwVehicleConnector.STREAM_NAME, Optional.empty())) {
            Page page = session.nextPage(2);
            assertThat(page.records()).isEmpty();
            assertThat(page.hasMore()).isFalse();
            assertThat(page.nextCursor()).isEmpty();
        }
    }

    @Test
    void paginatesWithKeysetAndDeduplicatesKeysAcrossPagesThroughLastPage() {
        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .withQueryParam("$limit", equalTo("2"))
                .withQueryParam("$order", equalTo("kenteken ASC"))
                .willReturn(aResponse().withStatus(200).withBody("""
                        [
                          {"kenteken":"AA0001","merk":"VOLVO","massa_ledig_voertuig":1400},
                          {"kenteken":"AA0002","merk":"DAF","massa_ledig_voertuig":7500}
                        ]
                        """)));

        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .withQueryParam("$limit", equalTo("2"))
                .withQueryParam("$order", equalTo("kenteken ASC"))
                .withQueryParam("$where", equalTo("kenteken > 'AA0002'"))
                .willReturn(aResponse().withStatus(200).withBody("""
                        [
                          {"kenteken":"AA0002","merk":"DAF","massa_ledig_voertuig":7500},
                          {"kenteken":"AA0003","merk":"TESLA","massa_ledig_voertuig":1850}
                        ]
                        """)));

        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .withQueryParam("$limit", equalTo("2"))
                .withQueryParam("$order", equalTo("kenteken ASC"))
                .withQueryParam("$where", equalTo("kenteken > 'AA0003'"))
                .willReturn(aResponse().withStatus(200).withBody("""
                        [
                          {"kenteken":"AA0004","merk":"BMW","massa_ledig_voertuig":1520}
                        ]
                        """)));

        try (SourceSession session = connector.openSession(RdwVehicleConnector.STREAM_NAME, Optional.empty())) {
            Page p1 = session.nextPage(2);
            assertThat(p1.records()).extracting(r -> r.primaryKey()).containsExactly("AA0001", "AA0002");
            assertThat(p1.nextCursor()).contains(Cursor.of("AA0002"));
            assertThat(p1.hasMore()).isTrue();

            Page p2 = session.nextPage(2);
            assertThat(p2.records()).extracting(r -> r.primaryKey()).containsExactly("AA0003");
            assertThat(p2.nextCursor()).contains(Cursor.of("AA0003"));
            assertThat(p2.hasMore()).isTrue();

            Page p3 = session.nextPage(2);
            assertThat(p3.records()).extracting(r -> r.primaryKey()).containsExactly("AA0004");
            assertThat(p3.nextCursor()).contains(Cursor.of("AA0004"));
            assertThat(p3.hasMore()).isFalse();
        }
    }

    @Test
    void honors429RetryAfterAnd500Recovery() {
        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .inScenario("retry-flow")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "0"))
                .willSetStateTo("after-429"));

        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .inScenario("retry-flow")
                .whenScenarioStateIs("after-429")
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo("after-500"));

        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .inScenario("retry-flow")
                .whenScenarioStateIs("after-500")
                .willReturn(aResponse().withStatus(200).withBody("""
                        [{"kenteken":"ZZ9999","merk":"AUDI"}]
                        """)));

        try (SourceSession session = connector.openSession(RdwVehicleConnector.STREAM_NAME, Optional.empty())) {
            Page page = session.nextPage(5);
            assertThat(page.records()).hasSize(1);
            assertThat(page.records().getFirst().primaryKey()).isEqualTo("ZZ9999");
            assertThat(connector.totalRetries()).isEqualTo(2L);
        }
    }

    @Test
    void rejectsMalformedJsonImmediatelyWithoutRetrying() {
        wm.stubFor(get(urlPathEqualTo("/resource/m9d7-ebf2.json"))
                .willReturn(aResponse().withStatus(200).withBody("{not-valid-json")));

        try (SourceSession session = connector.openSession(RdwVehicleConnector.STREAM_NAME, Optional.empty())) {
            assertThatThrownBy(() -> session.nextPage(10))
                    .isInstanceOf(ConnectorException.class)
                    .satisfies(ex -> {
                        ConnectorException ce = (ConnectorException) ex;
                        assertThat(ce.reasonCode()).isEqualTo("MALFORMED_PAYLOAD");
                        assertThat(ce.isRetryable()).isFalse();
                    });
            assertThat(connector.totalRetries()).isZero();
        }
    }
}
