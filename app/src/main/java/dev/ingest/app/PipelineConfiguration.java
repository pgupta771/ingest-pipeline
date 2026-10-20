package dev.ingest.app;

import dev.ingest.app.fixture.RdwFixtureServer;
import dev.ingest.connector.rdw.RdwConnectorConfig;
import dev.ingest.connector.rdw.RdwVehicleConnector;
import dev.ingest.core.ConnectorRegistry;
import java.io.IOException;
import java.net.URI;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Wires connectors into {@link ConnectorRegistry} and configures Kafka topics and the batch listener container.
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
            @Value("${ingest.fixture.enabled:false}") boolean fixtureEnabled,
            @Value("${ingest.fixture.port:18089}") int fixturePort) {
        ConnectorRegistry registry = new ConnectorRegistry();
        String effectiveUrl = fixtureEnabled ? ("http://127.0.0.1:" + fixturePort) : rdwBaseUrl;
        registry.register(new RdwVehicleConnector(RdwConnectorConfig.defaults(URI.create(effectiveUrl))));
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
