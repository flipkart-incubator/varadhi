package com.flipkart.varadhi.spi.inmemory;

import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.spi.services.Producer;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import lombok.Setter;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryProducer implements Producer {

    private final InMemoryStorageTopic topic;
    private final boolean roundRobinRouting;
    private final Timer scheduler;
    private final ConcurrentHashMap<Integer, InMemoryPartition> partitions;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    @Setter
    private volatile long produceLatencyMs = 0;

    public InMemoryProducer(InMemoryStorageTopic topic, boolean roundRobinRouting, HashedWheelTimer scheduler) {
        this.topic = topic;
        this.roundRobinRouting = roundRobinRouting;
        this.scheduler = scheduler;
        this.partitions = new ConcurrentHashMap<>();
        for (int i = 0; i < topic.getPartitions(); i++) {
            partitions.put(i, new InMemoryPartition());
        }
    }

    @Override
    public CompletableFuture<Offset> produceAsync(Message message) {
        int partition = selectPartition(message);

        InMemoryPartition partitionStore = partitions.get(partition);
        int offset = 0;
        try {
            offset = partitionStore.add(new SimpleMessage(message).serialize());
        } catch (IOException e) {
            throw new MessagingException("Failed to serialize message for topic: " + topic.getName(), e);
        }

        CompletableFuture<Offset> future = new CompletableFuture<>();
        InMemoryOffset result = new InMemoryOffset(partition, offset);

        if (produceLatencyMs > 0) {
            scheduler.newTimeout(timeout -> future.complete(result), produceLatencyMs, TimeUnit.MILLISECONDS);
        } else {
            future.complete(result);
        }
        return future;
    }

    private int selectPartition(Message message) {
        if (!roundRobinRouting) {
            if (message.getGroupId() != null) {
                return Math.abs(message.getGroupId().hashCode()) % topic.getPartitions();
            }
        }
        return roundRobinCounter.getAndIncrement() % topic.getPartitions();
    }

    @Override
    public void close() throws IOException {
        // No-op for in-memory producer
    }
}
