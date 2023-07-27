package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.entities.ProduceContext;
import com.flipkart.varadhi.produce.MsgProduceStatus;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.Constants.*;

public class ProduceMetricProvider {
    private final MeterRegistry meterRegistry;

    public ProduceMetricProvider(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        //TODO::Discuss should this be made optional (config driven)
        //TODO::Add required meters for metrics.
    }


    public void onMessageProduceEnd(
            long produceStartTime,
            MsgProduceStatus status,
            ProduceContext context
    ) {
    }

    private Map<String, String> getTags(ProduceContext context) {
        Map<String, String> tags = new HashMap<>();
        tags.put(TAG_NAME_REGION, context.getTopicContext().getRegion());
        tags.put(TAG_NAME_PROJECT, context.getTopicContext().getProjectName());
        tags.put(TAG_NAME_TOPIC, context.getTopicContext().getTopicName());
        String identity =
                null == context.getUserContext() ? UNKNOWN_IDENTITY_VALUE : context.getUserContext().getSubject();
        tags.put(TAG_NAME_IDENTITY, identity);
        return tags;
    }
}
