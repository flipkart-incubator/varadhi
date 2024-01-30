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

    public CustomHttpServerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void responseEnd(HttpRequest requestMetric, HttpResponse response, long bytesWritten) {

        Tags tags = MetricsUtil.getCustomHttpHeaders(requestMetric.headers());

        String resourceName = requestMetric.headers().get(MetricConstants.METRIC_NAME_FOR_API);
        if (StringUtils.isNotEmpty(resourceName)) {
            Counter.builder(resourceName)
                    .tags(tags)
                    .description("Api to Produce")
                    .register(meterRegistry)
                    .increment();
        }

        if (MetricsUtil.isSuccessfulResponse(response.statusCode())) {
            Counter.builder(MetricConstants.TOTAL_SUCCESS_COUNT)
                    .tags(tags)
                    .description("Total HTTP Success responses")
                    .register(meterRegistry);
        }
    }
}
