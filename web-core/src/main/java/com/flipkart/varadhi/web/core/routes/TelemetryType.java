package com.flipkart.varadhi.web.core.routes;

public record TelemetryType(boolean metrics, boolean logs, boolean traces) {
    public static final TelemetryType DEFAULT = new TelemetryType(false, true, true);
    public static final TelemetryType ALL = new TelemetryType(true, true, true);
    public static final TelemetryType NONE = new TelemetryType(false, false, false);
}
