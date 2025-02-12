package com.flipkart.varadhi;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class LoggingSpanExporter implements SpanExporter {
    private final AtomicBoolean isShutdown = new AtomicBoolean();

    /**
     * Class constructor.
     *
     * @deprecated Use {@link #create()}.
     */
    @Deprecated
    public LoggingSpanExporter() {
    }

    public static LoggingSpanExporter create() {
        return new LoggingSpanExporter();
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        if (isShutdown.get()) {
            return CompletableResultCode.ofFailure();
        }

        // We always have 32 + 16 + name + several whitespace, 60 seems like an OK initial guess.
        StringBuilder sb = new StringBuilder(60);
        for (SpanData span : spans) {
            sb.setLength(0);
            sb.append("'")
              .append(span.getName())
              .append("' ")
              .append(": traceId=")
              .append(span.getTraceId())
              .append(", spanId=")
              .append(span.getSpanId())
              .append(", parentSpanId=")
              .append(span.getParentSpanId())
              .append(", kind=")
              .append(span.getKind())
              .append(", attr='")
              .append(
                  span.getAttributes()
                      .asMap()
                      .entrySet()
                      .stream()
                      .map(Object::toString)
                      .collect(Collectors.joining(","))
              )
              .append("', events='")
              .append(span.getEvents().stream().map(Object::toString).collect(Collectors.joining(",")))
              .append("', time=")
              .append((span.getEndEpochNanos() - span.getStartEpochNanos()) / 1000L)
              .append(" μs");
            log.info(sb.toString());
        }
        return CompletableResultCode.ofSuccess();
    }

    /**
     * Flushes the data.
     *
     * @return the result of the operation
     */
    @Override
    public CompletableResultCode flush() {
        CompletableResultCode resultCode = new CompletableResultCode();
        return resultCode.succeed();
    }

    @Override
    public CompletableResultCode shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.info("Calling shutdown() multiple times.");
            return CompletableResultCode.ofSuccess();
        }
        return flush();
    }
}
