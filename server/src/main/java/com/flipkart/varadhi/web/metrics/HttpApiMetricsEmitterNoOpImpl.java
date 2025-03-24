package com.flipkart.varadhi.web.metrics;

public final class HttpApiMetricsEmitterNoOpImpl implements HttpApiMetricsEmitter {

    @Override
    public void recordSuccess(int statusCode) {
        // No-op
    }

    @Override
    public void recordError(int statusCode, String errorType) {
        // No-op
    }

    @Override
    public void close() {
        // No-op
    }
}
