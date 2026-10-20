package dev.ingest.app;

import dev.ingest.connector.rdw.RdwConnectorConfig;
import dev.ingest.connector.rdw.RdwVehicleConnector;
import dev.ingest.core.ConnectorRegistry;
import java.net.URI;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Wires connectors into {@link ConnectorRegistry} and configures the batch Kafka listener container.
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
    public ConnectorRegistry connectorRegistry(
            @Value("${ingest.rdw.base-url:https://opendata.rdw.nl}") String rdwBaseUrl) {
        ConnectorRegistry registry = new ConnectorRegistry();
        registry.register(new RdwVehicleConnector(RdwConnectorConfig.defaults(URI.create(rdwBaseUrl))));
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
