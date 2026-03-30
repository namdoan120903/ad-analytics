package com.adanalytics.infrastructure.adapter.source;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.domain.model.AdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * ClickHouse data source adapter (stub implementation).
 * <p>
 * In production, this would use ClickHouse JDBC driver with streaming:
 * 
 * <pre>
 * ClickHouseProperties properties = new ClickHouseProperties();
 * properties.setMaxBlockSize(65536);
 * ClickHouseDataSource ds = new ClickHouseDataSource(url, properties);
 * // Use ClickHouseStatement.executeQueryClickHouseResponse() for streaming
 * </pre>
 * 
 * ClickHouse is optimized for analytical queries and can push aggregation to
 * the DB.
 */
public class ClickHouseDataSourceAdapter implements DataSourcePort {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseDataSourceAdapter.class);

    private final String jdbcUrl;

    public ClickHouseDataSourceAdapter(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public Stream<AdRecord> streamRecords() {
        log.warn("ClickHouse adapter is a stub. Configure ClickHouse JDBC driver to use.");
        throw new UnsupportedOperationException(
                "ClickHouse adapter not yet implemented. "
                        + "Add clickhouse-jdbc dependency and implement streaming query. "
                        + "Consider pushing aggregation to ClickHouse for optimal performance.");
    }

    @Override
    public String getSourceName() {
        return "ClickHouse [" + jdbcUrl + "]";
    }
}
