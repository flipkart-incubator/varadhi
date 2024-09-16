package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.ratelimit.LoadInfo;
import com.flipkart.varadhi.entities.ratelimit.TrafficData;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

/**
 * This class will capture incoming traffic usage by providing a method to add topic usage by producers.
 * This class will accumualte everything and send to the controller for further processing for rate limit.
 * Post that, reset the usage for the topic.
 */
@Slf4j
public class TrafficAggregator {

    @Getter
    private LoadInfo previousLoad;
    private final int frequency;
    private final LoadInfo loadInfo;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final MessageExchange exchange;

    public TrafficAggregator(MessageExchange exchange, int frequency) {
        this.exchange = exchange;
        this.frequency = frequency;
        loadInfo = new LoadInfo(UUID.randomUUID(), System.currentTimeMillis(), System.currentTimeMillis(),
                new ConcurrentHashMap<>()
        );
        previousLoad = loadInfo;
        sendUsageToController();
    }

    public void addTopicUsage(String topic, long throughput, long qps) {
        loadInfo.getTopicUsageMap().compute(topic, (k, v) -> {
            if (v == null) {
                return TrafficData.builder().throughputIn(throughput).rateIn(qps).build();
            } else {
                // because we are updating the values, uuid shows data has changed
                loadInfo.setUuid(UUID.randomUUID());
                v.setRateIn(v.getRateIn() + qps);
                v.setThroughputIn(v.getThroughputIn() + throughput);
                return v;
            }
        });
    }

    public void clear() {
        loadInfo.setFrom(System.currentTimeMillis());
        loadInfo.getTopicUsageMap().replaceAll((k, v) -> TrafficData.builder().throughputIn(0L).rateIn(0L).build());
        loadInfo.getTopicUsageMap().forEach((k, v) -> {
            v.setRateIn(0L);
            v.setThroughputIn(0L);
        });
    }

    public void sendUsageToController() {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            log.info("Sending usage to controller");
            updateLocalHistory();
            sendTrafficUsage();
            clear();
        }, 0, frequency, TimeUnit.SECONDS);

    }

    private void updateLocalHistory() {
        previousLoad = loadInfo;
    }

    private void sendTrafficUsage() {
        loadInfo.setTo(System.currentTimeMillis());
        ClusterMessage msg = ClusterMessage.of(loadInfo);
        exchange.send(ROUTE_CONTROLLER, "collect", msg);
    }

    public boolean allowProduce(String topic, float suppressionFactor) {
        if (suppressionFactor == 0) {
            return true;
        }
        if (previousLoad == null || loadInfo == null) {
            return true;
        }
        TrafficData prevData = previousLoad.getTopicUsageMap().get(topic);
        TrafficData currentData = loadInfo.getTopicUsageMap().get(topic);
        if(prevData == null || currentData == null) {
            // no information about load for current topic, cant rate limit using suppression factor
            return true;
        }
        return !(prevData.getThroughputIn() * suppressionFactor < currentData.getThroughputIn() ||
                prevData.getRateIn() * suppressionFactor < currentData.getRateIn());
    }

}
