package com.adanalytics.integration;

import com.adanalytics.infrastructure.adapter.source.CsvDataSourceAdapter;
import com.adanalytics.domain.model.AdRecord;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge case tests for CSV parsing and data handling.
 */
class EdgeCaseTest {

    private static final Path TEST_DIR = Path.of("target/test-edge-cases");

    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectories(TEST_DIR);
    }

    @AfterEach
    void tearDown() throws IOException {
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
    @DisplayName("Skips malformed rows gracefully")
    void skipsMalformedRows() throws IOException {
        Path csvFile = TEST_DIR.resolve("malformed.csv");
        Files.write(csvFile, List.of(
                "campaign_id,date,impressions,clicks,spend,conversions",
                "CMP001,2025-01-01,10000,500,100.00,20", // valid
                "CMP002,2025-01-01,bad_number,500,100.00,20", // invalid number
                "CMP003,2025-01-01", // too few fields
                "CMP004,2025-01-01,5000,200,50.00,10", // valid
                ",2025-01-01,5000,200,50.00,10", // blank campaign_id
                "CMP005,2025-01-01,8000,400,75.00,15" // valid
        ));

        CsvDataSourceAdapter adapter = new CsvDataSourceAdapter(csvFile, false);
        List<AdRecord> records = adapter.streamRecords().collect(Collectors.toList());

        assertEquals(3, records.size(), "Should parse only 3 valid rows");
        assertEquals("CMP001", records.get(0).campaignId());
        assertEquals("CMP004", records.get(1).campaignId());
        assertEquals("CMP005", records.get(2).campaignId());
        assertTrue(adapter.getSkippedRowCount() > 0, "Should have recorded skipped rows");
    }

    @Test
    @DisplayName("Handles zero impressions (CTR null)")
    void handlesZeroImpressions() throws IOException {
        Path csvFile = TEST_DIR.resolve("zero_impressions.csv");
        Files.write(csvFile, List.of(
                "campaign_id,date,impressions,clicks,spend,conversions",
                "CMP001,2025-01-01,0,0,100.00,0"));

        CsvDataSourceAdapter adapter = new CsvDataSourceAdapter(csvFile, false);
        List<AdRecord> records = adapter.streamRecords().collect(Collectors.toList());

        assertEquals(1, records.size());
        assertEquals(0, records.get(0).impressions());
        assertEquals(0, records.get(0).clicks());
    }

    @Test
    @DisplayName("Handles zero conversions (CPA null)")
    void handlesZeroConversions() throws IOException {
        Path csvFile = TEST_DIR.resolve("zero_conversions.csv");
        Files.write(csvFile, List.of(
                "campaign_id,date,impressions,clicks,spend,conversions",
                "CMP001,2025-01-01,10000,500,100.00,0"));

        CsvDataSourceAdapter adapter = new CsvDataSourceAdapter(csvFile, false);
        List<AdRecord> records = adapter.streamRecords().collect(Collectors.toList());

        assertEquals(1, records.size());
        assertEquals(0, records.get(0).conversions());
    }

    @Test
    @DisplayName("Handles empty file")
    void handlesEmptyFile() throws IOException {
        Path csvFile = TEST_DIR.resolve("empty.csv");
        Files.write(csvFile, List.of(
                "campaign_id,date,impressions,clicks,spend,conversions"));

        CsvDataSourceAdapter adapter = new CsvDataSourceAdapter(csvFile, false);
        List<AdRecord> records = adapter.streamRecords().collect(Collectors.toList());

        assertTrue(records.isEmpty(), "Empty file should produce no records");
    }

    @Test
    @DisplayName("Handles file without header")
    void handlesNoHeader() throws IOException {
        Path csvFile = TEST_DIR.resolve("no_header.csv");
        Files.write(csvFile, List.of(
                "CMP001,2025-01-01,10000,500,100.00,20",
                "CMP002,2025-01-01,5000,200,50.00,10"));

        CsvDataSourceAdapter adapter = new CsvDataSourceAdapter(csvFile, false);
        List<AdRecord> records = adapter.streamRecords().collect(Collectors.toList());

        assertEquals(2, records.size(), "Should parse both data rows");
    }

    @Test
    @DisplayName("Memory-mapped handles malformed rows")
    void memoryMappedMalformedRows() throws IOException {
        Path csvFile = TEST_DIR.resolve("mmap_malformed.csv");
        Files.write(csvFile, List.of(
                "campaign_id,date,impressions,clicks,spend,conversions",
                "CMP001,2025-01-01,10000,500,100.00,20",
                "INVALID_ROW",
                "CMP002,2025-01-01,5000,200,50.00,10"));

        CsvDataSourceAdapter adapter = new CsvDataSourceAdapter(csvFile, true);
        List<AdRecord> records = adapter.streamRecords().collect(Collectors.toList());

        assertEquals(2, records.size(), "Memory-mapped should also skip malformed rows");
    }
}
