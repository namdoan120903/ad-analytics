package com.adanalytics.application.usecase;

import com.adanalytics.domain.model.CampaignSnapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Selects the Top-K elements from a collection using a min/max-heap.
 * Time complexity: O(N log K) where N = total elements, K = desired count.
 * Space complexity: O(K).
 */
public final class TopKSelector {

    private TopKSelector() {
        // utility class
    }

    /**
     * Select top K campaigns by CTR (descending — highest CTR first).
     */
    public static List<CampaignSnapshot> topByCtrDescending(
            Iterable<CampaignSnapshot> snapshots, int k) {

        // Min-heap: smallest CTR at top, so we evict the smallest when heap exceeds K
        PriorityQueue<CampaignSnapshot> heap = new PriorityQueue<>(k + 1,
                Comparator.comparingDouble(s -> s.ctr() != null ? s.ctr() : -1.0));

        for (CampaignSnapshot s : snapshots) {
            if (s.ctr() == null)
                continue; // skip campaigns with no impressions
            heap.offer(s);
            if (heap.size() > k) {
                heap.poll(); // remove smallest
            }
        }

        List<CampaignSnapshot> result = new ArrayList<>(heap);
        result.sort((a, b) -> Double.compare(
                b.ctr() != null ? b.ctr() : 0,
                a.ctr() != null ? a.ctr() : 0)); // descending
        return result;
    }

    /**
     * Select top K campaigns by CPA (ascending — lowest CPA first).
     * Excludes campaigns with 0 conversions (CPA is null).
     */
    public static List<CampaignSnapshot> topByCpaAscending(
            Iterable<CampaignSnapshot> snapshots, int k) {

        // Max-heap: largest CPA at top, so we evict the largest when heap exceeds K
        PriorityQueue<CampaignSnapshot> heap = new PriorityQueue<>(k + 1,
                (a, b) -> Double.compare(
                        b.cpa() != null ? b.cpa() : Double.MAX_VALUE,
                        a.cpa() != null ? a.cpa() : Double.MAX_VALUE));

        for (CampaignSnapshot s : snapshots) {
            if (s.cpa() == null)
                continue; // exclude 0-conversion campaigns
            heap.offer(s);
            if (heap.size() > k) {
                heap.poll(); // remove largest
            }
        }

        List<CampaignSnapshot> result = new ArrayList<>(heap);
        result.sort(Comparator.comparingDouble(s -> s.cpa() != null ? s.cpa() : Double.MAX_VALUE)); // ascending
        return result;
    }
}
