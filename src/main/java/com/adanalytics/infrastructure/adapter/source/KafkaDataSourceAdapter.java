package com.adanalytics.infrastructure.adapter.source;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.domain.model.AdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Kafka data source adapter (stub implementation).
 * <p>
 * In production, this would use KafkaConsumer with polling:
 * 
 * <pre>
 * KafkaConsumer consumer = new KafkaConsumer<>(props);
 * consumer.subscribe(List.of("ad-records"));
 * // Poll records in batches and bridge to Stream API
 * // Use Spliterator-based iterator wrapping consumer.poll()
 * </pre>
 * 
 * For real-time streaming, consider using Kafka Streams or reactive Kafka.
 */
public class KafkaDataSourceAdapter implements DataSourcePort {

    private static final Logger log = LoggerFactory.getLogger(KafkaDataSourceAdapter.class);

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;

    public KafkaDataSourceAdapter(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
    }

    @Override
    public Stream<AdRecord> streamRecords() {
        log.warn("Kafka adapter is a stub. Configure kafka-clients dependency to use.");
        throw new UnsupportedOperationException(
                "Kafka adapter not yet implemented. "
                        + "Add kafka-clients dependency and implement consumer polling bridge. "
                        + "Consider backpressure handling for high-throughput topics.");
    }

    @Override
    public String getSourceName() {
        return "Kafka [" + bootstrapServers + "/" + topic + "]";
    }
}
