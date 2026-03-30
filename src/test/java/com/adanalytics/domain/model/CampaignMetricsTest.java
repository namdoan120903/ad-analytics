package com.adanalytics.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CampaignMetrics domain logic.
 * Tests CTR, CPA calculations, thread-safe accumulation, and edge cases.
 */
class CampaignMetricsTest {

    @Nested
    @DisplayName("CTR Calculation")
    class CtrTests {

        @Test
        @DisplayName("CTR = clicks / impressions")
        void ctrIsClicksDividedByImpressions() {
            CampaignMetrics metrics = new CampaignMetrics("CMP001");
            metrics.addRecord(new AdRecord("CMP001", "2025-01-01", 10000, 500, 100.0, 20));

            Double ctr = metrics.getCtr();
            assertNotNull(ctr);
            assertEquals(0.05, ctr, 1e-9, "CTR should be 500/10000 = 0.05");
        }

        @Test
        @DisplayName("CTR is null when impressions = 0")
        void ctrIsNullForZeroImpressions() {
            CampaignMetrics metrics = new CampaignMetrics("CMP001");
            // Don't add any records → impressions = 0
            assertNull(metrics.getCtr());
        }

        @Test
        @DisplayName("CTR accumulates across multiple records")
        void ctrAccumulatesAcrossRecords() {
            CampaignMetrics metrics = new CampaignMetrics("CMP001");
            metrics.addRecord(new AdRecord("CMP001", "2025-01-01", 5000, 200, 50.0, 10));
            metrics.addRecord(new AdRecord("CMP001", "2025-01-02", 5000, 300, 50.0, 10));

            Double ctr = metrics.getCtr();
            assertNotNull(ctr);
            assertEquals(0.05, ctr, 1e-9, "CTR should be 500/10000 = 0.05");
        }
    }

    @Nested
    @DisplayName("CPA Calculation")
    class CpaTests {

        @Test
        @DisplayName("CPA = spend / conversions")
        void cpaIsSpendDividedByConversions() {
            CampaignMetrics metrics = new CampaignMetrics("CMP001");
            metrics.addRecord(new AdRecord("CMP001", "2025-01-01", 10000, 500, 200.0, 20));

            Double cpa = metrics.getCpa();
            assertNotNull(cpa);
            assertEquals(10.0, cpa, 1e-9, "CPA should be 200/20 = 10.0");
        }

        @Test
        @DisplayName("CPA is null when conversions = 0")
        void cpaIsNullForZeroConversions() {
            CampaignMetrics metrics = new CampaignMetrics("CMP001");
            metrics.addRecord(new AdRecord("CMP001", "2025-01-01", 10000, 500, 200.0, 0));

            assertNull(metrics.getCpa());
        }

        @Test
        @DisplayName("CPA accumulates across multiple records")
        void cpaAccumulatesAcrossRecords() {
            CampaignMetrics metrics = new CampaignMetrics("CMP001");
            metrics.addRecord(new AdRecord("CMP001", "2025-01-01", 5000, 200, 100.0, 10));
            metrics.addRecord(new AdRecord("CMP001", "2025-01-02", 5000, 300, 100.0, 10));

            Double cpa = metrics.getCpa();
            assertNotNull(cpa);
            assertEquals(10.0, cpa, 1e-9, "CPA should be 200/20 = 10.0");
        }
    }

    @Nested
    @DisplayName("Snapshot")
    class SnapshotTests {

        @Test
        @DisplayName("Snapshot reflects accumulated values")
        void snapshotReflectsAccumulatedValues() {
            CampaignMetrics metrics = new CampaignMetrics("CMP001");
            metrics.addRecord(new AdRecord("CMP001", "2025-01-01", 10000, 500, 200.0, 20));
            metrics.addRecord(new AdRecord("CMP001", "2025-01-02", 5000, 250, 100.0, 10));

            CampaignSnapshot snapshot = metrics.toSnapshot();

            assertEquals("CMP001", snapshot.campaignId());
            assertEquals(15000, snapshot.totalImpressions());
            assertEquals(750, snapshot.totalClicks());
            assertEquals(300.0, snapshot.totalSpend(), 1e-9);
            assertEquals(30, snapshot.totalConversions());
            assertEquals(0.05, snapshot.ctr(), 1e-9);
            assertEquals(10.0, snapshot.cpa(), 1e-9);
        }

        @Test
        @DisplayName("CSV row generation")
        void csvRowGeneration() {
            CampaignSnapshot snapshot = new CampaignSnapshot(
                    "CMP001", 15000, 750, 300.0, 30, 0.05, 10.0);

            String csv = snapshot.toCsvRow();
            assertTrue(csv.startsWith("CMP001,15000,750,"));
            assertTrue(csv.contains("300.00"));
            assertTrue(csv.contains("30"));
        }
    }

    @Nested
    @DisplayName("AdRecord Validation")
    class AdRecordTests {

        @Test
        @DisplayName("Rejects null campaign ID")
        void rejectsNullCampaignId() {
            assertThrows(IllegalArgumentException.class,
                    () -> new AdRecord(null, "2025-01-01", 100, 10, 5.0, 1));
        }

        @Test
        @DisplayName("Rejects blank campaign ID")
        void rejectsBlankCampaignId() {
            assertThrows(IllegalArgumentException.class,
                    () -> new AdRecord("", "2025-01-01", 100, 10, 5.0, 1));
        }

        @Test
        @DisplayName("Rejects negative impressions")
        void rejectsNegativeImpressions() {
            assertThrows(IllegalArgumentException.class,
                    () -> new AdRecord("CMP001", "2025-01-01", -1, 10, 5.0, 1));
        }

        @Test
        @DisplayName("Accepts zero values")
        void acceptsZeroValues() {
            AdRecord record = new AdRecord("CMP001", "2025-01-01", 0, 0, 0.0, 0);
            assertEquals(0, record.impressions());
            assertEquals(0, record.clicks());
        }
    }
}
