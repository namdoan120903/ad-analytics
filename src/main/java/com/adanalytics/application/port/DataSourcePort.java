package com.adanalytics.application.port;

import com.adanalytics.domain.model.AdRecord;

import java.util.stream.Stream;

/**
 * Port for streaming advertising records from any data source.
 * Implementations must return a lazy Stream to avoid loading all data into
 * memory.
 * <p>
 * Pluggable: new data sources can be added by implementing this interface
 * and registering via configuration — no core logic changes required.
 */
public interface DataSourcePort {

    /**
     * Stream ad records lazily from the underlying data source.
     * The caller is responsible for closing the stream.
     *
     * @return a lazy Stream of AdRecord
     */
    Stream<AdRecord> streamRecords();

    /**
     * @return human-readable name of this data source (e.g., "CSV", "PostgreSQL")
     */
    String getSourceName();
}
