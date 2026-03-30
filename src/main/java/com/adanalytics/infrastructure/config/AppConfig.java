package com.adanalytics.infrastructure.config;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.application.port.ResultSinkPort;
import com.adanalytics.application.usecase.AggregateCampaignUseCase;
import com.adanalytics.infrastructure.adapter.sink.CsvResultSinkAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * Spring configuration wiring the Clean Architecture components.
 */
@Configuration
public class AppConfig {

    @Value("${datasource.type:csv}")
    private String datasourceType;

    @Value("${datasource.csv.input-path:ad_data.csv}")
    private String inputPath;

    @Value("${datasource.csv.use-memory-mapped:false}")
    private boolean useMemoryMapped;

    @Value("${output.directory:results/}")
    private String outputDirectory;

    @Bean
    public DataSourcePort dataSourcePort() {
        return DataSourceFactory.create(datasourceType, inputPath, useMemoryMapped);
    }

    @Bean
    public ResultSinkPort resultSinkPort() {
        return new CsvResultSinkAdapter(Path.of(outputDirectory));
    }

    @Bean
    public AggregateCampaignUseCase aggregateCampaignUseCase(
            DataSourcePort dataSourcePort, ResultSinkPort resultSinkPort) {
        return new AggregateCampaignUseCase(dataSourcePort, resultSinkPort);
    }
}
