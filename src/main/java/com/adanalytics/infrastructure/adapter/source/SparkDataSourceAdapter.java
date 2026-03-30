package com.adanalytics.infrastructure.adapter.source;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.domain.model.AdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Apache Spark data source adapter (stub implementation).
 * <p>
 * In production, this would use SparkSession to read distributed data:
 * 
 * <pre>
 * SparkSession spark = SparkSession.builder()
 *         .appName("AdAnalytics")
 *         .master("spark://master:7077")
 *         .getOrCreate();
 * Dataset&lt;Row&gt; df = spark.read().csv("hdfs://data/ad_records.csv");
 * // Convert Dataset to Java Stream via collectAsList() or toLocalIterator()
 * </pre>
 * 
 * For very large datasets (TB+), push aggregation to Spark and collect only
 * results.
 */
public class SparkDataSourceAdapter implements DataSourcePort {

    private static final Logger log = LoggerFactory.getLogger(SparkDataSourceAdapter.class);

    private final String masterUrl;
    private final String inputPath;

    public SparkDataSourceAdapter(String masterUrl, String inputPath) {
        this.masterUrl = masterUrl;
        this.inputPath = inputPath;
    }

    @Override
    public Stream<AdRecord> streamRecords() {
        log.warn("Spark adapter is a stub. Configure Spark dependencies to use.");
        throw new UnsupportedOperationException(
                "Spark adapter not yet implemented. "
                        + "Add spark-core and spark-sql dependencies. "
                        + "For TB-scale data, push aggregation to Spark rather than streaming rows to JVM.");
    }

    @Override
    public String getSourceName() {
        return "Spark [" + masterUrl + ", path=" + inputPath + "]";
    }
}
