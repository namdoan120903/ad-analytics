package com.adanalytics.application.port;

import com.adanalytics.domain.model.CampaignSnapshot;

import java.util.List;

/**
 * Port for writing aggregated campaign results to any output sink.
 * Pluggable: CSV files, databases, message queues, etc.
 */
public interface ResultSinkPort {

    /**
     * Write a list of campaign snapshots to the designated output.
     *
     * @param filename the output file/resource name
     * @param results  the campaign snapshots to write
     * @param headers  CSV column headers
     */
    void write(String filename, List<CampaignSnapshot> results, String[] headers);
}
