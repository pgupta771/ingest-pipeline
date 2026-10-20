package dev.ingest.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the resumable ingestion pipeline application.
 */
@SpringBootApplication(scanBasePackages = "dev.ingest")
public class IngestPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestPipelineApplication.class, args);
    }
}
