1. Initial Problem Understanding

Build a production-grade Java application that processes a large CSV dataset (~1GB) containing advertising performance records.

The system must efficiently process large data (no full file load into memory), compute aggregated campaign metrics, and output results as CSV.

Use Clean Architecture + Hexagonal (Ports & Adapters). Support pluggable data sources (CSV, PostgreSQL, ClickHouse, Kafka, Spark). Provide CLI + REST API.

Optimize for performance, memory, and scalability.

2. Architecture Design

Help me design a Clean Architecture for a Java data pipeline that processes large CSV files (~1GB). I want clear separation between Domain, Application, Ports, Infrastructure, and Presentation layers.

How should I structure packages and dependencies?

3. CSV Streaming Processing

How can I process a large CSV file (~1GB) in Java without loading the entire file into memory?

Compare BufferedReader vs FileChannel + MappedByteBuffer.

Which approach is better for performance?

4. Efficient CSV Parsing

String.split() is slow for large-scale processing.

How can I implement efficient CSV parsing in Java (low allocation, high throughput)?

5. Aggregation Strategy

I need to aggregate metrics by campaign_id:

impressions
clicks
spend
conversions

How to design a high-performance aggregation using ConcurrentHashMap?

Should I use LongAdder / DoubleAdder?

6. Multi-threading Design

Design a producer-consumer pipeline for processing CSV rows in parallel.

How to batch records (e.g. 10k rows per batch) and process them using multiple threads?

7. Top-K Computation

I need to get:

Top 10 campaigns by CTR (descending)
Top 10 campaigns by CPA (ascending)

What is the most efficient way to compute Top-K using PriorityQueue?

Time complexity should be O(N log K).

8. CLI Design

How to implement a CLI in Java that accepts parameters like:

--input ad_data.csv
--output results/
--source csv

9. REST API Design

Expose results via Spring Boot REST API:

GET /analytics/top-ctr
GET /analytics/top-cpa

Return JSON response.

How to structure controller and service layers?

10. DataSource Abstraction

I want to support multiple data sources:

CSV
PostgreSQL
ClickHouse
Kafka
Spark

How to design a DataSourcePort interface and implement adapters for each?

Use factory pattern to resolve datasource via config.

11. Error Handling

How to handle malformed CSV rows safely without breaking the pipeline?

What is the best practice for logging and skipping invalid records?

12. Performance Optimization

How to optimize:

Memory usage for 1GB CSV processing
GC behavior
Throughput

What JVM options should I tune?