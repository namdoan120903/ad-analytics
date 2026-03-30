package com.adanalytics.domain.model;

/**
 * Immutable snapshot of aggregated campaign metrics.
 * Used for output serialization and API responses.
 */
public record CampaignSnapshot(
        String campaignId,
        long totalImpressions,
        long totalClicks,
        double totalSpend,
        long totalConversions,
        Double ctr,
        Double cpa) {
    /**
     * Format as CSV row.
     */
    public String toCsvRow() {
        return String.join(",",
                campaignId,
                String.valueOf(totalImpressions),
                String.valueOf(totalClicks),
                String.format("%.2f", totalSpend),
                String.valueOf(totalConversions),
                ctr != null ? String.format("%.6f", ctr) : "N/A",
                cpa != null ? String.format("%.2f", cpa) : "N/A");
    }
}
