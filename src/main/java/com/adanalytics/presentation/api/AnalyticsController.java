package com.adanalytics.presentation.api;

import com.adanalytics.application.usecase.AggregateCampaignUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API controller exposing analytics endpoints.
 * <p>
 * GET /analytics/top-ctr → Top 10 campaigns by CTR (descending)
 * GET /analytics/top-cpa → Top 10 campaigns by CPA (ascending, excludes
 * 0-conversion)
 */
@RestController
@RequestMapping("/analytics")
public class AnalyticsController {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsController.class);

    private final AggregateCampaignUseCase useCase;

    public AnalyticsController(AggregateCampaignUseCase useCase) {
        this.useCase = useCase;
    }

    @GetMapping("/top-ctr")
    public ResponseEntity<AnalyticsResponse> getTopCtr() {
        log.info("API request: GET /analytics/top-ctr");
        try {
            AggregateCampaignUseCase.AggregationResult result = useCase.aggregate();
            AnalyticsResponse response = AnalyticsResponse.from(
                    result.topCtr(), result.totalRowsProcessed(), result.uniqueCampaigns());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error processing top-ctr request", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/top-cpa")
    public ResponseEntity<AnalyticsResponse> getTopCpa() {
        log.info("API request: GET /analytics/top-cpa");
        try {
            AggregateCampaignUseCase.AggregationResult result = useCase.aggregate();
            AnalyticsResponse response = AnalyticsResponse.from(
                    result.topCpa(), result.totalRowsProcessed(), result.uniqueCampaigns());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error processing top-cpa request", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
