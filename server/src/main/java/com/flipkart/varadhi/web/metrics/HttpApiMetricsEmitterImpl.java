package com.flipkart.varadhi.web.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@link HttpApiMetricsEmitter} that records HTTP API metrics using Micrometer.
 * This class handles recording of latency and request counts for both successful and failed API calls.
 *
 * <p>The metrics recorded include:
 * <ul>
 *   <li>Request latency (timer)</li>
 *   <li>Total request count (counter)</li>
 *   <li>Error count (counter)</li>
 * </ul>
 *
 * <p>All metrics are tagged with:
 * <ul>
 *   <li>API name</li>
 *   <li>HTTP status category (2xx, 4xx, 5xx)</li>
 *   <li>HTTP status code (the exact status code)</li>
 *   <li>Error type (for failed requests)</li>
 * </ul>
 */
@Slf4j
public final class HttpApiMetricsEmitterImpl implements HttpApiMetricsEmitter {
    private static final String METRIC_PREFIX = "varadhi.http.requests.";
    private static final String TAG_API = "api";
    private static final String TAG_STATUS_CATEGORY = "status_category";
    private static final String TAG_STATUS_CODE = "status_code";
    private static final String TAG_ERROR_TYPE = "error_type";

    private final MeterRegistry meterRegistry;
    private final List<Tag> baseTags;
    private final Timer.Sample timerSample;

    /**
     * Constructs a new HttpApiMetricsEmitterImpl.
     *
     * @param meterRegistry the meter registry to record metrics
     * @param apiName       the name of the API being monitored
     * @param tags          additional tags to be included with all metrics
     * @throws NullPointerException if meterRegistry, apiName, or tags is null
     */
    public HttpApiMetricsEmitterImpl(MeterRegistry meterRegistry, String apiName, List<Tag> tags) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        Objects.requireNonNull(apiName, "apiName must not be null");
        this.baseTags = new ArrayList<>(Objects.requireNonNull(tags, "tags must not be null"));
        this.baseTags.add(Tag.of(TAG_API, apiName));
        this.timerSample = Timer.start(meterRegistry);
    }

    /**
     * Records metrics for a successful API request.
     *
     * @param statusCode the HTTP status code of the response
     */
    @Override
    public void recordSuccess(int statusCode) {
        var tags = new ArrayList<>(baseTags);
        tags.add(Tag.of(TAG_STATUS_CATEGORY, categorizeStatusCode(statusCode)));
        tags.add(Tag.of(TAG_STATUS_CODE, String.valueOf(statusCode)));

        recordMetrics(tags);
    }

    /**
     * Records metrics for a failed API request.
     *
     * @param statusCode the HTTP status code of the response
     * @param errorType the type of error that occurred
     */
    @Override
    public void recordError(int statusCode, String errorType) {
        var tags = new ArrayList<>(baseTags);
        tags.add(Tag.of(TAG_STATUS_CATEGORY, categorizeStatusCode(statusCode)));
        tags.add(Tag.of(TAG_STATUS_CODE, String.valueOf(statusCode)));
        tags.add(Tag.of(TAG_ERROR_TYPE, errorType));

        recordMetrics(tags);
        recordErrorMetrics(tags);
    }

    /**
     * Categorizes HTTP status codes into standard groups (2xx, 4xx, 5xx).
     *
     * @param statusCode the HTTP status code to categorize
     * @return the category of the status code
     */
    private String categorizeStatusCode(int statusCode) {
        return switch (statusCode / 100) {
            case 2 -> "2xx";
            case 4 -> "4xx";
            case 5 -> "5xx";
            default -> String.valueOf(statusCode);
        };
    }

    /**
     * Records common metrics for both successful and failed requests.
     *
     * @param tags the tags to be included with the metrics
     */
    private void recordMetrics(List<Tag> tags) {
        // Record latency
        timerSample.stop(
            Timer.builder(METRIC_PREFIX + "latency")
                 .tags(tags)
                 .description("API request latency")
                 .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99, 0.999)
                 .register(meterRegistry)
        );

        // Record request count
        Counter.builder(METRIC_PREFIX + "total")
               .tags(tags)
               .description("Total number of API requests")
               .register(meterRegistry)
               .increment();
    }

    /**
     * Records additional metrics specific to failed requests.
     *
     * @param tags the tags to be included with the metrics
     */
    private void recordErrorMetrics(List<Tag> tags) {
        Counter.builder(METRIC_PREFIX + "errors")
               .tags(tags)
               .description("Total number of API errors")
               .register(meterRegistry)
               .increment();
    }

    @Override
    public void close() {
        // No cleanup needed
    }
}
