package com.flipkart.varadhi.metrices;

import com.flipkart.varadhi.Constants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.MultiMap;
import io.vertx.core.spi.metrics.HttpServerMetrics;
import io.vertx.core.spi.observability.HttpRequest;
import io.vertx.core.spi.observability.HttpResponse;

import java.util.Arrays;

public class CustomHttpServerMetrics  implements HttpServerMetrics<HttpRequest, HttpResponse, Void> {

    private final MeterRegistry meterRegistry;

    public CustomHttpServerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public HttpRequest requestBegin(Void socketMetric, HttpRequest request) {

        MultiMap multiMap = request.headers();
        String topicName = multiMap.get(Constants.Tags.TAG_NAME_TOPIC);
        Tag customTag = Tag.of(Constants.Tags.TAG_NAME_TOPIC, topicName);

        Timer.Sample sample = Timer.start(meterRegistry);
        sample.stop(Timer.builder("http.requests")
                .tags(Arrays.asList(customTag))
                .register(meterRegistry));
        return HttpServerMetrics.super.requestBegin(socketMetric, request);
    }

    @Override
    public void responseEnd(HttpRequest requestMetric, HttpResponse response, long bytesWritten) {

        MultiMap multiMap = requestMetric.headers();
        String topicName = multiMap.get(Constants.Tags.TAG_NAME_TOPIC);
        Tag customTag = Tag.of(Constants.Tags.TAG_NAME_TOPIC, topicName);
        Tag responseCodeTag = Tag.of("responseCodeCategory", categorizeStatusCode(response.statusCode()));

        Timer.builder("http.requests")
                .tags(Arrays.asList(customTag, responseCodeTag))
                .register(meterRegistry);
//                .record(response.statusCode());
        HttpServerMetrics.super.responseEnd(requestMetric, response, bytesWritten);

    }

    private String categorizeStatusCode(int statusCode) {
        if (statusCode >= 200 && statusCode < 300) {
            return "2xx";
        } else if (statusCode >= 400 && statusCode < 500) {
            return "4xx";
        } else if (statusCode >= 500) {
            return "5xx";
        }
        return String.valueOf(statusCode);
    }
}
