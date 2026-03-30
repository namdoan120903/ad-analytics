package com.adanalytics.domain.model;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe, mutable accumulator for campaign-level aggregated metrics.
 * Uses atomic adders for lock-free concurrent accumulation.
 * <p>
 * This is a domain object containing pure business logic for metric
 * calculations.
 */
public class CampaignMetrics {

    private final String campaignId;
    private final LongAdder totalImpressions = new LongAdder();
    private final LongAdder totalClicks = new LongAdder();
    private final DoubleAdder totalSpend = new DoubleAdder();
    private final LongAdder totalConversions = new LongAdder();

    public CampaignMetrics(String campaignId) {
        this.campaignId = campaignId;
    }

    /**
     * Accumulate a single ad record into this campaign's metrics.
     * Thread-safe — can be called concurrently from multiple threads.
     */
    public void addRecord(AdRecord record) {
        totalImpressions.add(record.impressions());
        totalClicks.add(record.clicks());
        totalSpend.add(record.spend());
        totalConversions.add(record.conversions());
    }

    /**
     * Click-Through Rate = totalClicks / totalImpressions.
     * Returns null if totalImpressions is 0 (avoid division by zero).
     */
    public Double getCtr() {
        long impressions = totalImpressions.sum();
        if (impressions == 0) {
            return null;
        }
        return (double) totalClicks.sum() / impressions;
    }

    /**
     * Cost Per Acquisition = totalSpend / totalConversions.
     * Returns null if totalConversions is 0 (avoid division by zero).
     */
    public Double getCpa() {
        long conversions = totalConversions.sum();
        if (conversions == 0) {
            return null;
        }
        return totalSpend.sum() / conversions;
    }

    /**
     * Create an immutable snapshot of the current state for output/serialization.
     */
    public CampaignSnapshot toSnapshot() {
        return new CampaignSnapshot(
                campaignId,
                totalImpressions.sum(),
                totalClicks.sum(),
                totalSpend.sum(),
                totalConversions.sum(),
                getCtr(),
                getCpa());
    }

    public String getCampaignId() {
        return campaignId;
    }

    public long getTotalImpressions() {
        return totalImpressions.sum();
    }

    public long getTotalClicks() {
        return totalClicks.sum();
    }

    public double getTotalSpend() {
        return totalSpend.sum();
    }

    public long getTotalConversions() {
        return totalConversions.sum();
    }
}
