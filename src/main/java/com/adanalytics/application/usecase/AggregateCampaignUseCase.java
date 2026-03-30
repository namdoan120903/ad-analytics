package com.adanalytics.application.usecase;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.application.port.ResultSinkPort;
import com.adanalytics.domain.model.AdRecord;
import com.adanalytics.domain.model.CampaignMetrics;
import com.adanalytics.domain.model.CampaignSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Core use case: aggregate ad records by campaign, then extract top-K results.
 * <p>
 * Memory: O(K) where K = number of unique campaigns.
 * Time: O(N) for aggregation + O(K log 10) for top-K selection.
 * <p>
 * Thread-safe aggregation via ConcurrentHashMap + LongAdder/DoubleAdder in
 * CampaignMetrics.
 */
public class AggregateCampaignUseCase {

    private static final Logger log = LoggerFactory.getLogger(AggregateCampaignUseCase.class);
    private static final int TOP_K = 10;
    private static final String[] CSV_HEADERS = {
            "campaign_id", "total_impressions", "total_clicks",
            "total_spend", "total_conversions", "CTR", "CPA"
    };

    private final DataSourcePort dataSource;
    private final ResultSinkPort resultSink;

    public AggregateCampaignUseCase(DataSourcePort dataSource, ResultSinkPort resultSink) {
        this.dataSource = dataSource;
        this.resultSink = resultSink;
    }

    /**
     * Execute the full pipeline: stream → aggregate → select top-K → write output.
     */
    public ExecutionResult execute() {
        log.info("Starting aggregation pipeline from source: {}", dataSource.getSourceName());

        ConcurrentHashMap<String, CampaignMetrics> metricsMap = new ConcurrentHashMap<>();
        LongAdder processedCount = new LongAdder();
        LongAdder errorCount = new LongAdder();

        long startTime = System.nanoTime();

        // Phase 1: Stream and aggregate
        try (Stream<AdRecord> stream = dataSource.streamRecords()) {
            stream.forEach(record -> {
                try {
                    metricsMap.computeIfAbsent(record.campaignId(), CampaignMetrics::new)
                            .addRecord(record);
                    processedCount.increment();
                } catch (Exception e) {
                    errorCount.increment();
                    if (errorCount.sum() <= 10) {
                        log.warn("Error processing record for campaign {}: {}",
                                record.campaignId(), e.getMessage());
                    }
                }
            });
        }

        long aggregationTime = System.nanoTime() - startTime;

        log.info("Aggregation complete: {} records processed, {} errors, {} unique campaigns",
                processedCount.sum(), errorCount.sum(), metricsMap.size());

        // Phase 2: Create snapshots
        List<CampaignSnapshot> snapshots = new ArrayList<>(metricsMap.size());
        metricsMap.values().forEach(m -> snapshots.add(m.toSnapshot()));

        // Phase 3: Top-K selection
        List<CampaignSnapshot> topCtr = TopKSelector.topByCtrDescending(snapshots, TOP_K);
        List<CampaignSnapshot> topCpa = TopKSelector.topByCpaAscending(snapshots, TOP_K);

        // Phase 4: Write results
        resultSink.write("top10_ctr.csv", topCtr, CSV_HEADERS);
        resultSink.write("top10_cpa.csv", topCpa, CSV_HEADERS);

        long totalTime = System.nanoTime() - startTime;

        ExecutionResult result = new ExecutionResult(
                processedCount.sum(),
                errorCount.sum(),
                metricsMap.size(),
                totalTime / 1_000_000, // ms
                aggregationTime / 1_000_000,
                topCtr,
                topCpa);

        log.info("Pipeline complete: {} ms total, throughput: {} rows/sec",
                result.totalTimeMs(),
                result.totalRows() > 0 ? (result.totalRows() * 1000L / result.totalTimeMs()) : 0);

        return result;
    }

    /**
     * Execute aggregation only (for REST API — does not write to sink).
     */
    public AggregationResult aggregate() {
        log.info("Starting aggregation from source: {}", dataSource.getSourceName());

        ConcurrentHashMap<String, CampaignMetrics> metricsMap = new ConcurrentHashMap<>();
        LongAdder processedCount = new LongAdder();

        try (Stream<AdRecord> stream = dataSource.streamRecords()) {
            stream.forEach(record -> {
                metricsMap.computeIfAbsent(record.campaignId(), CampaignMetrics::new)
                        .addRecord(record);
                processedCount.increment();
            });
        }

        List<CampaignSnapshot> snapshots = new ArrayList<>(metricsMap.size());
        metricsMap.values().forEach(m -> snapshots.add(m.toSnapshot()));

        List<CampaignSnapshot> topCtr = TopKSelector.topByCtrDescending(snapshots, TOP_K);
        List<CampaignSnapshot> topCpa = TopKSelector.topByCpaAscending(snapshots, TOP_K);

        return new AggregationResult(topCtr, topCpa, processedCount.sum(), metricsMap.size());
    }

    /**
     * Result of a full CLI execution.
     */
    public record ExecutionResult(
            long totalRows,
            long errorRows,
            int uniqueCampaigns,
            long totalTimeMs,
            long aggregationTimeMs,
            List<CampaignSnapshot> topCtr,
            List<CampaignSnapshot> topCpa) {
    }

    /**
     * Result of aggregation (for API responses).
     */
    public record AggregationResult(
            List<CampaignSnapshot> topCtr,
            List<CampaignSnapshot> topCpa,
            long totalRowsProcessed,
            int uniqueCampaigns) {
    }
}
