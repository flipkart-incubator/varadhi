package com.flipkart.varadhi.metrices;

import com.flipkart.varadhi.utils.MetricsUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.spi.metrics.HttpServerMetrics;
import io.vertx.core.spi.observability.HttpRequest;
import io.vertx.core.spi.observability.HttpResponse;

public class CustomHttpServerMetrics  implements HttpServerMetrics<HttpServerRequest, HttpServerResponse, Void> {

    private final MeterRegistry meterRegistry;
    private final Counter totalRequest;
    private final Counter totalSuccessResponse;
    private final Counter totalFailureResponse;
    private final Timer requestTimer;

    public CustomHttpServerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        totalRequest = Counter.builder(MetricConstants.TOTAL_REQUEST_COUNT)
                .description("Total number of HTTP requests")
                .register(meterRegistry);
        totalFailureResponse = Counter.builder(MetricConstants.TOTAL_FAILURE_COUNT)
                .description("Total HTTP Failure responses")
                .register(meterRegistry);
        totalSuccessResponse = Counter.builder(MetricConstants.TOTAL_SUCCESS_COUNT)
                .description("Total HTTP Success responses")
                .register(meterRegistry);
        this.requestTimer = Timer.builder(MetricConstants.DURATION)
                .description("Response time of HTTP requests")
                .register(meterRegistry);
    }

    @Override
    public HttpServerRequest requestBegin(Void socketMetric, HttpRequest request) {

        Tags tags = MetricsUtil.getCustomHttpHeaders(request.headers());
        Counter.builder(MetricConstants.INPUT_REQUEST)
                .description("Request Received")
                .tags(tags)
                .register(meterRegistry)
                .increment();

        totalRequest.increment();
        return null;
    }

    @Override
    public void responseEnd(HttpServerRequest requestMetric, HttpResponse response, long bytesWritten) {

        Tags tags = MetricsUtil.getCustomHttpHeaders(requestMetric.headers());
        Counter.builder(MetricConstants.OUTPUT_RESPONSE_CODE)
                .description("Output Returned")
                .tags(tags)
                .tag(MetricConstants.RESPONSE_CODE, categorizeStatusCode(response.statusCode()))
                .register(meterRegistry)
                .increment();

        if (isSuccessfulResponse(response.statusCode())) {
            totalSuccessResponse.increment();
        } else {
            totalFailureResponse.increment();
        }
    }

    private Boolean isSuccessfulResponse(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private String categorizeStatusCode(int statusCode) {
        if (isSuccessfulResponse(statusCode)) {
            return "2xx";
        } else if (statusCode >= 400 && statusCode < 500) {
            return "4xx";
        } else if (statusCode >= 500) {
            return "5xx";
        }
        return String.valueOf(statusCode);
    }
}
