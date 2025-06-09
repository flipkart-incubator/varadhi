package com.flipkart.varadhi.web;


import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.ContextStorage;
import io.vertx.tracing.opentelemetry.VertxContextStorageProvider;

public class SpanProvider {
    Tracer tracer;
    ContextStorage contextStorage;

    public SpanProvider(Tracer tracer) {
        this.tracer = tracer;
        this.contextStorage = new VertxContextStorageProvider().get();

    }

    public SpanBuilder newSpan(String name) {
        return tracer.spanBuilder(name).setParent(contextStorage.current());
    }
}
