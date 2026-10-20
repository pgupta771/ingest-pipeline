package dev.ingest.app;

import dev.ingest.app.fixture.RdwFixtureServer;
import dev.ingest.connector.ecb.EcbXmlConnector;
import dev.ingest.connector.rdw.RdwConnectorConfig;
import dev.ingest.connector.rdw.RdwVehicleConnector;
import dev.ingest.core.ConnectorRegistry;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Wires connectors (`rdw` and `ecb-xml`) into {@link ConnectorRegistry} and configures Kafka topics.
 */
@Configuration
public class PipelineConfiguration {

    @Bean
    public NewTopic rdwVehiclesTopic() {
        return TopicBuilder.name("ingest.raw.rdw_vehicles")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ecbExchangeRatesTopic() {
        return TopicBuilder.name("ingest.raw.ecb_exchange_rates")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name("ingest.dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "ingest.fixture.enabled", havingValue = "true")
    public RdwFixtureServer embeddedRdwFixtureServer(
            @Value("${ingest.fixture.port:18089}") int port,
            @Value("${ingest.fixture.records:200000}") int totalRecords) throws IOException {
        return new RdwFixtureServer(port, totalRecords);
    }

    @Bean
    public ConnectorRegistry connectorRegistry(
            @Value("${ingest.rdw.base-url:https://opendata.rdw.nl}") String rdwBaseUrl,
            @Value("${ingest.ecb.xml-path:/tmp/eurofxref-hist.xml}") String ecbXmlPath,
            @Value("${ingest.fixture.enabled:false}") boolean fixtureEnabled,
            @Value("${ingest.fixture.port:18089}") int fixturePort) {
        ConnectorRegistry registry = new ConnectorRegistry();
        String effectiveUrl = fixtureEnabled ? ("http://127.0.0.1:" + fixturePort) : rdwBaseUrl;
        registry.register(new RdwVehicleConnector(RdwConnectorConfig.defaults(URI.create(effectiveUrl))));
        registry.register(new EcbXmlConnector(Path.of(ecbXmlPath)));
        return registry;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setIdleBetweenPolls(50L);
        return factory;
    }
}
