package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: right now the MessageTracker is holding the reference to it, to be able to start the timer. That may / may not
 * be ideal. Due to this, the ProcessingLoop knows about when to start and stop and the tracker knows about how to do
 * that.
 * Maybe in alternative approach, processingLoop can have the metricsReference and "set" the tracker into the tracker.
 * This way the tracker remains light weight, but its interface will then have the tracker info.
 */
public class ConsumerMetrics implements AutoCloseable {
    private final MeterRegistry meterRegistry;

    // looks overkill
    private final Map<InternalQueueType, EnumMap<MessageConsumptionStatus, Timer>> timers = new HashMap<>();

    public ConsumerMetrics(MeterRegistry meterRegistry, String subName, int shardId, InternalQueueType[] queueTypes) {
        this.meterRegistry = meterRegistry;
        for (InternalQueueType queueType : queueTypes) {
            EnumMap<MessageConsumptionStatus, Timer> t = new EnumMap<>(MessageConsumptionStatus.class);
            for (MessageConsumptionStatus status : MessageConsumptionStatus.values()) {
                t.put(status, createTimer(subName, shardId, queueType, status));
            }
            this.timers.put(queueType, t);
        }
    }

    Timer createTimer(String subName, int shardId, InternalQueueType queueType, MessageConsumptionStatus status) {
        return Timer.builder("consume.%s".formatted(subName))
                    .tag("shard", String.valueOf(shardId))
                    .tag("queue", queueType.toString())
                    .tag("status", status.toString())
                    .register(meterRegistry);
    }

    public Tracker begin(InternalQueueType queueType) {
        return new Tracker(queueType);
    }

    @RequiredArgsConstructor
    public class Tracker {
        private final InternalQueueType queueType;
        private final Timer.Sample sample = Timer.start(meterRegistry);

        public void end(MessageConsumptionStatus status) {
            sample.stop(timers.get(queueType).get(status));
        }
    }

    @Override
    public void close() {
        // deregister all timers
        timers.values().forEach(m -> m.values().forEach(meterRegistry::remove));
    }
}
