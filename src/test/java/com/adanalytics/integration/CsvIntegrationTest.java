package com.adanalytics.integration;

import com.adanalytics.application.usecase.AggregateCampaignUseCase;
import com.adanalytics.infrastructure.adapter.sink.CsvResultSinkAdapter;
import com.adanalytics.infrastructure.adapter.source.CsvDataSourceAdapter;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test: end-to-end with a small CSV file.
 */
class CsvIntegrationTest {

    private static final Path TEST_DIR = Path.of("target/test-integration");
    private Path testCsvPath;

    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectories(TEST_DIR);
        testCsvPath = TEST_DIR.resolve("test_data.csv");

        // Create small test CSV
        List<String> lines = List.of(
                "campaign_id,date,impressions,clicks,spend,conversions",
                "CMP001,2025-01-01,10000,500,100.00,20",
                "CMP001,2025-01-02,10000,600,150.00,30",
                "CMP002,2025-01-01,5000,400,80.00,10",
                "CMP002,2025-01-02,5000,300,70.00,15",
                "CMP003,2025-01-01,20000,100,200.00,5",
                "CMP004,2025-01-01,8000,800,300.00,0", // 0 conversions
                "CMP005,2025-01-01,15000,750,250.00,50",
                "CMP006,2025-01-01,3000,600,50.00,25",
                "CMP007,2025-01-01,12000,120,180.00,8",
                "CMP008,2025-01-01,9000,900,400.00,40",
                "CMP009,2025-01-01,7000,350,120.00,18",
                "CMP010,2025-01-01,6000,60,90.00,3",
                "CMP011,2025-01-01,11000,1100,500.00,55");
        Files.write(testCsvPath, lines);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up
        Files.walk(TEST_DIR)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
    }

    @Test
    @DisplayName("End-to-end: reads CSV, aggregates, writes output files")
    void endToEndCsvProcessing() throws IOException {
        Path outputDir = TEST_DIR.resolve("output");

        CsvDataSourceAdapter source = new CsvDataSourceAdapter(testCsvPath, false);
        CsvResultSinkAdapter sink = new CsvResultSinkAdapter(outputDir);
        AggregateCampaignUseCase useCase = new AggregateCampaignUseCase(source, sink);

        AggregateCampaignUseCase.ExecutionResult result = useCase.execute();

        // Verify processing
        assertEquals(13, result.totalRows(), "Should process 13 data rows");
        assertEquals(0, result.errorRows());
        assertEquals(11, result.uniqueCampaigns());

        // Verify output files exist
        Path ctrFile = outputDir.resolve("top10_ctr.csv");
        Path cpaFile = outputDir.resolve("top10_cpa.csv");
        assertTrue(Files.exists(ctrFile), "top10_ctr.csv should exist");
        assertTrue(Files.exists(cpaFile), "top10_cpa.csv should exist");

        // Verify CTR file
        List<String> ctrLines = Files.readAllLines(ctrFile);
        assertEquals(11, ctrLines.size(), "Header + 10 data rows");
        assertEquals("campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA",
                ctrLines.get(0));

        // Verify CPA file does not contain CMP004 (0 conversions)
        List<String> cpaLines = Files.readAllLines(cpaFile);
        assertTrue(cpaLines.size() >= 2, "Should have header + data");
        for (int i = 1; i < cpaLines.size(); i++) {
            assertFalse(cpaLines.get(i).startsWith("CMP004"),
                    "CPA output should exclude campaigns with 0 conversions");
        }
    }

    @Test
    @DisplayName("Memory-mapped mode works correctly")
    void memoryMappedMode() {
        CsvDataSourceAdapter source = new CsvDataSourceAdapter(testCsvPath, true);
        CsvResultSinkAdapter sink = new CsvResultSinkAdapter(TEST_DIR.resolve("mmap_output"));
        AggregateCampaignUseCase useCase = new AggregateCampaignUseCase(source, sink);

        AggregateCampaignUseCase.ExecutionResult result = useCase.execute();

        assertEquals(13, result.totalRows(), "Memory-mapped should process same rows");
        assertEquals(11, result.uniqueCampaigns());
    }
}
