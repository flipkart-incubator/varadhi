package com.flipkart.varadhi.metrices;

import com.flipkart.varadhi.utils.MetricsUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.vertx.core.spi.metrics.HttpServerMetrics;
import io.vertx.core.spi.observability.HttpRequest;
import io.vertx.core.spi.observability.HttpResponse;
import org.apache.commons.lang3.StringUtils;

public class CustomHttpServerMetrics  implements HttpServerMetrics<HttpRequest, HttpResponse, Void> {

    private final MeterRegistry meterRegistry;
    private final Counter totalRequest;
    private final Counter totalSuccessResponse;
    private final Counter totalFailureResponse;

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
    }

    @Override
    public HttpRequest requestBegin(Void socketMetric, HttpRequest request) {

        Tags tags = MetricsUtil.getCustomHttpHeaders(request.headers());
        Counter.builder(MetricConstants.INPUT_REQUEST)
                .description("Request Received")
                .tags(tags)
                .register(meterRegistry)
                .increment();

        totalRequest.increment();

        Counter.builder(MetricsUtil.getRequestInitials(request.uri()))
                .tags(tags)
                .register(meterRegistry)
                .increment();
        return request;
    }

    @Override
    public void responseEnd(HttpRequest requestMetric, HttpResponse response, long bytesWritten) {

        Tags tags = MetricsUtil.getCustomHttpHeaders(requestMetric.headers());
        String resourceName = requestMetric.headers().get(MetricConstants.RESOURCE_NAME);
        if (StringUtils.isNotEmpty(resourceName)) {
            Counter.builder(resourceName)
                    .description("Api to Produce")
                    .register(meterRegistry)
                    .increment();
        }

        Counter.builder(MetricConstants.OUTPUT_RESPONSE_CODE)
                .description("Output Returned")
                .tags(tags)
                .tag(MetricConstants.RESPONSE_CODE, MetricsUtil.categorizeStatusCode(response.statusCode()))
                .register(meterRegistry)
                .increment();

        if (MetricsUtil.isSuccessfulResponse(response.statusCode())) {
            totalSuccessResponse.increment();
        } else {
            totalFailureResponse.increment();
        }
    }
}
