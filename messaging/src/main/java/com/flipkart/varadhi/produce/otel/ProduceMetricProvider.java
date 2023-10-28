package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.entities.ProduceContext;
import com.flipkart.varadhi.entities.ProduceResult;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.Constants.Tags.*;

public class ProduceMetricProvider {
    private final MeterRegistry meterRegistry;

    public ProduceMetricProvider(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        //TODO::Discuss should this be made optional (config driven)
        //TODO::Add required meters for metrics.
    }

    public void OnProduceEnd(
            String messageId,
            ProduceResult.Status status,
            long producerLatency,
            ProduceContext context
    ) {
        Map<String, String> tags = getTags(context);
    }

    private Map<String, String> getTags(ProduceContext context) {
        Map<String, String> tags = new HashMap<>();
        tags.put(TAG_NAME_REGION, context.getTopicContext().getRegion());
        tags.put(TAG_NAME_PROJECT, context.getTopicContext().getProject());
        tags.put(TAG_NAME_TOPIC, context.getTopicContext().getTopic());
        String identity = context.getRequestContext().getProduceIdentity();
        tags.put(TAG_NAME_IDENTITY, identity);
        return tags;
    }
}
