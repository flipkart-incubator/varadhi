package com.flipkart.varadhi.core.config;

import com.flipkart.varadhi.common.exceptions.InvalidConfigException;

import java.util.HashMap;
import java.util.List;

public class MetricsExporterOptions extends HashMap<String, String> {

    List<String> ALLOWED_EXPORTERS = List.of("prometheus", "jmx", "otlp");

    public String getExporter() {
        String value = get("exporter");
        if (value == null || value.isEmpty()) {
            throw new InvalidConfigException("Exporter must be specified in metrics options");
        }

        value = value.toLowerCase();
        if (!ALLOWED_EXPORTERS.contains(value)) {
            throw new InvalidConfigException(
                String.format("Exporter must be one of %s, but found %s", String.join(",", ALLOWED_EXPORTERS), value)
            );
        }
        return value;
    }
}
