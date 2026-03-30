package com.adanalytics.infrastructure.adapter.sink;

import com.adanalytics.application.port.ResultSinkPort;
import com.adanalytics.domain.model.CampaignSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes aggregated campaign results to CSV files using BufferedWriter.
 */
public class CsvResultSinkAdapter implements ResultSinkPort {

    private static final Logger log = LoggerFactory.getLogger(CsvResultSinkAdapter.class);

    private final Path outputDirectory;

    public CsvResultSinkAdapter(Path outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    @Override
    public void write(String filename, List<CampaignSnapshot> results, String[] headers) {
        Path outputPath = outputDirectory.resolve(filename);
        try {
            Files.createDirectories(outputDirectory);
            try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
                // Write header
                writer.write(String.join(",", headers));
                writer.newLine();

                // Write data rows
                for (CampaignSnapshot snapshot : results) {
                    writer.write(snapshot.toCsvRow());
                    writer.newLine();
                }
            }
            log.info("Written {} records to {}", results.size(), outputPath);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write results to " + outputPath, e);
        }
    }
}
