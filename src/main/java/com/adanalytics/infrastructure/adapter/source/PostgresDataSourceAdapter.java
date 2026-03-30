package com.adanalytics.infrastructure.adapter.source;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.domain.model.AdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * PostgreSQL data source adapter (stub implementation).
 * <p>
 * In production, this would use JDBC with cursor-based streaming:
 * 
 * <pre>
 * connection.setAutoCommit(false);
 * statement.setFetchSize(5000);
 * ResultSet rs = statement.executeQuery("SELECT * FROM ad_records");
 * </pre>
 * 
 * The ResultSet is then wrapped in a Stream using Spliterators.
 */
public class PostgresDataSourceAdapter implements DataSourcePort {

    private static final Logger log = LoggerFactory.getLogger(PostgresDataSourceAdapter.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public PostgresDataSourceAdapter(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public Stream<AdRecord> streamRecords() {
        log.warn("PostgreSQL adapter is a stub. Configure JDBC dependencies and connection to use.");
        throw new UnsupportedOperationException(
                "PostgreSQL adapter not yet implemented. "
                        + "Add postgresql JDBC driver dependency and implement cursor-based streaming. "
                        + "See class javadoc for implementation pattern.");
    }

    @Override
    public String getSourceName() {
        return "PostgreSQL [" + jdbcUrl + "]";
    }
}
