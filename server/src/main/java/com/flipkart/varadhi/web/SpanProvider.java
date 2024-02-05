package com.flipkart.varadhi.web;


import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.ContextStorage;
import io.vertx.tracing.opentelemetry.VertxContextStorageProvider;

public class SpanProvider {
    public final String appName = "Varadhi";
    Tracer tracer;
    ContextStorage contextStorage;
    public SpanProvider(Tracer tracer) {
        this.tracer = tracer;
        this.contextStorage = new VertxContextStorageProvider().get();

    }
    public Span getSpan(String name, String message) {
        return tracer.spanBuilder(appName).setParent(contextStorage.current()).setAttribute(name, message).startSpan();
    }

    public void emitSpan(String name, String message) {
        getSpan(name, message).end();
    }

}
