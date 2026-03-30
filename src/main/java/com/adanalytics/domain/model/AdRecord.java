package com.adanalytics.domain.model;

/**
 * Immutable value object representing a single advertising performance record.
 * No framework dependencies — pure domain object.
 */
public record AdRecord(
        String campaignId,
        String date,
        long impressions,
        long clicks,
        double spend,
        long conversions) {
    public AdRecord {
        if (campaignId == null || campaignId.isBlank()) {
            throw new IllegalArgumentException("campaignId must not be null or blank");
        }
        if (impressions < 0 || clicks < 0 || spend < 0 || conversions < 0) {
            throw new IllegalArgumentException("Metrics must not be negative");
        }
    }
}
