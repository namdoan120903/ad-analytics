package com.adanalytics.infrastructure.config;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.infrastructure.adapter.source.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Factory for resolving data source adapters based on configuration.
 * Implements the Factory pattern — adding a new data source requires only
 * a new case in this factory + a new adapter class implementing DataSourcePort.
 */
public class DataSourceFactory {

    private static final Logger log = LoggerFactory.getLogger(DataSourceFactory.class);

    /**
     * Create a DataSourcePort based on the given type and configuration.
     *
     * @param type         one of: csv, postgres, clickhouse, kafka, spark
     * @param inputPath    file path for CSV source
     * @param memoryMapped whether to use memory-mapped IO for CSV
     * @return configured DataSourcePort implementation
     */
    public static DataSourcePort create(String type, String inputPath, boolean memoryMapped) {
        log.info("Creating data source adapter: type={}", type);

        return switch (type.toLowerCase()) {
            case "csv" -> new CsvDataSourceAdapter(Path.of(inputPath), memoryMapped);

            case "postgres" -> new PostgresDataSourceAdapter(
                    System.getProperty("datasource.postgres.url", "jdbc:postgresql://localhost:5432/addb"),
                    System.getProperty("datasource.postgres.username", "postgres"),
                    System.getProperty("datasource.postgres.password", ""));

            case "clickhouse" -> new ClickHouseDataSourceAdapter(
                    System.getProperty("datasource.clickhouse.url", "jdbc:clickhouse://localhost:8123/addb"));

            case "kafka" -> new KafkaDataSourceAdapter(
                    System.getProperty("datasource.kafka.bootstrap-servers", "localhost:9092"),
                    System.getProperty("datasource.kafka.topic", "ad-records"),
                    System.getProperty("datasource.kafka.group-id", "ad-analytics"));

            case "spark" -> new SparkDataSourceAdapter(
                    System.getProperty("datasource.spark.master", "local[*]"),
                    inputPath);

            default -> throw new IllegalArgumentException(
                    "Unsupported datasource type: " + type
                            + ". Supported: csv, postgres, clickhouse, kafka, spark");
        };
    }
}
