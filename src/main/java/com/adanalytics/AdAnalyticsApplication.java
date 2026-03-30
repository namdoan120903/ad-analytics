package com.adanalytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Ad Analytics Pipeline application.
 * <p>
 * Modes:
 * <ul>
 * <li>CLI: java -jar app.jar --input data.csv --output results/</li>
 * <li>API: java -jar app.jar (starts Spring Boot web server on port 8080)</li>
 * </ul>
 */
@SpringBootApplication
public class AdAnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdAnalyticsApplication.class, args);
    }
}
