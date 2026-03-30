package com.adanalytics.presentation.cli;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.application.port.ResultSinkPort;
import com.adanalytics.application.usecase.AggregateCampaignUseCase;
import com.adanalytics.infrastructure.adapter.sink.CsvResultSinkAdapter;
import com.adanalytics.infrastructure.config.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

/**
 * CLI interface for the ad analytics pipeline.
 * <p>
 * Usage: java -jar ad-analytics-pipeline.jar --input ad_data.csv --output
 * results/ --source csv
 * <p>
 * When CLI args are provided, the pipeline runs in batch mode and exits.
 * When no args are provided, the REST API server starts instead.
 */
@Component
public class CliRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(CliRunner.class);

    @Override
    public void run(String... args) throws Exception {
        // Parse CLI arguments
        String inputPath = null;
        String outputDir = "results/";
        String sourceType = "csv";
        boolean memoryMapped = true;
        boolean cliMode = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input" -> {
                    if (i + 1 < args.length)
                        inputPath = args[++i];
                    cliMode = true;
                }
                case "--output" -> {
                    if (i + 1 < args.length)
                        outputDir = args[++i];
                }
                case "--source" -> {
                    if (i + 1 < args.length)
                        sourceType = args[++i];
                }
                case "--no-mmap" -> memoryMapped = false;
                case "--help" -> {
                    printUsage();
                    return;
                }
            }
        }

        if (!cliMode) {
            log.info("No --input argument provided. Running in API server mode.");
            return; // Let Spring Boot start the web server
        }

        if (inputPath == null) {
            log.error("--input argument is required in CLI mode");
            printUsage();
            return;
        }

        log.info("=== Ad Analytics Pipeline (CLI Mode) ===");
        log.info("Input:  {}", inputPath);
        log.info("Output: {}", outputDir);
        log.info("Source: {}", sourceType);
        log.info("Memory-mapped: {}", memoryMapped);

        // Wire components manually for CLI (independent of Spring DI for the data
        // source)
        DataSourcePort dataSource = DataSourceFactory.create(sourceType, inputPath, memoryMapped);
        ResultSinkPort resultSink = new CsvResultSinkAdapter(Path.of(outputDir));
        AggregateCampaignUseCase useCase = new AggregateCampaignUseCase(dataSource, resultSink);

        // Execute pipeline
        long startTime = System.currentTimeMillis();
        AggregateCampaignUseCase.ExecutionResult result = useCase.execute();
        long endTime = System.currentTimeMillis();

        // Print benchmark results
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

        log.info("=== Benchmark Results ===");
        log.info("Total rows processed: {}", result.totalRows());
        log.info("Error rows:           {}", result.errorRows());
        log.info("Unique campaigns:     {}", result.uniqueCampaigns());
        log.info("Aggregation time:     {} ms", result.aggregationTimeMs());
        log.info("Total time:           {} ms", result.totalTimeMs());
        log.info("Throughput:           {} rows/sec",
                result.totalRows() > 0 ? (result.totalRows() * 1000L / result.totalTimeMs()) : 0);
        log.info("Memory usage:         {} MB", usedMemory);
        log.info("=========================");

        // Print top results summary
        log.info("Top 10 by CTR (descending):");
        result.topCtr().forEach(s -> log.info("  {} — CTR: {}", s.campaignId(),
                s.ctr() != null ? String.format("%.4f%%", s.ctr() * 100) : "N/A"));

        log.info("Top 10 by CPA (ascending):");
        result.topCpa().forEach(s -> log.info("  {} — CPA: {}", s.campaignId(),
                s.cpa() != null ? String.format("$%.2f", s.cpa()) : "N/A"));
    }

    private void printUsage() {
        System.out.println("""
                Ad Analytics Pipeline

                Usage:
                  java -jar ad-analytics-pipeline.jar --input <file> [options]

                Options:
                  --input <file>     Input CSV file path (required for CLI mode)
                  --output <dir>     Output directory (default: results/)
                  --source <type>    Data source type: csv|postgres|clickhouse|kafka|spark (default: csv)
                  --no-mmap          Disable memory-mapped IO (use BufferedReader instead)
                  --help             Show this help message

                Examples:
                  java -jar ad-analytics-pipeline.jar --input ad_data.csv --output results/ --source csv
                  java -jar ad-analytics-pipeline.jar  (starts REST API server)
                """);
    }
}
