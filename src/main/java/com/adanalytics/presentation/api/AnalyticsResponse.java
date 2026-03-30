package com.adanalytics.presentation.api;

import com.adanalytics.domain.model.CampaignSnapshot;

import java.util.List;

/**
 * JSON response wrapper for analytics API endpoints.
 */
public record AnalyticsResponse(
        List<CampaignEntry> campaigns,
        long totalRowsProcessed,
        int uniqueCampaigns) {
    /**
     * Single campaign entry in the API response.
     */
    public record CampaignEntry(
            String campaignId,
            long totalImpressions,
            long totalClicks,
            double totalSpend,
            long totalConversions,
            Double ctr,
            Double cpa) {
        public static CampaignEntry from(CampaignSnapshot snapshot) {
            return new CampaignEntry(
                    snapshot.campaignId(),
                    snapshot.totalImpressions(),
                    snapshot.totalClicks(),
                    snapshot.totalSpend(),
                    snapshot.totalConversions(),
                    snapshot.ctr(),
                    snapshot.cpa());
        }
    }

    public static AnalyticsResponse from(
            List<CampaignSnapshot> snapshots, long totalRows, int uniqueCampaigns) {
        List<CampaignEntry> entries = snapshots.stream()
                .map(CampaignEntry::from)
                .toList();
        return new AnalyticsResponse(entries, totalRows, uniqueCampaigns);
    }
}
