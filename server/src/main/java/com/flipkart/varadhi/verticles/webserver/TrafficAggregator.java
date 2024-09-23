package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.qos.entity.LoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.entity.TrafficData;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.core.cluster.ControllerApi.ROUTE_CONTROLLER;

/**
 * This class will capture incoming traffic usage by providing a method to add topic usage by producers.
 * This class will accumualte everything and send to the controller for further processing for rate limit.
 * Post that, reset the usage for the topic.
 */
@Slf4j
public class TrafficAggregator {

    private LoadInfo loadInfo;
    private final int frequency;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final MessageExchange exchange;
    private final RateLimiterService rateLimiterService;

    public TrafficAggregator(MessageExchange exchange, String clientId, int frequency, RateLimiterService rateLimiterService) {
        this.exchange = exchange;
        this.frequency = frequency;
        this.rateLimiterService = rateLimiterService;
        loadInfo = new LoadInfo(clientId, System.currentTimeMillis(), System.currentTimeMillis(),
                new ConcurrentHashMap<>()
        );
        sendUsageToController();
    }

    public void addTopicUsage(String topic, long throughput, long qps) {
        loadInfo.getTopicUsageMap().compute(topic, (k, v) -> {
            if (v == null) {
                return TrafficData.builder().throughputIn(throughput).rateIn(qps).build();
            } else {
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
            try {
                log.info("Sending usage to controller");
                sendTrafficUsage();
                clear();
            } catch (Exception e) {
                log.error("Error while sending usage to controller", e);
            }
        }, frequency, frequency, TimeUnit.SECONDS);

    }

    private void sendTrafficUsage() {
        loadInfo.setTo(System.currentTimeMillis());
        ClusterMessage msg = ClusterMessage.of(loadInfo);
        log.info("Sending message to controller: {}", loadInfo);
        CompletableFuture<SuppressionData<SuppressionFactor>> response =
                exchange.request(ROUTE_CONTROLLER, "collect", msg).thenApply(rm -> rm.getResponse(
                        SuppressionData.class));
        response.whenComplete((suppressionData, throwable) -> {
            if (throwable != null) {
                log.error("Error while receiving suppression data from controller", throwable);
            } else {
                log.info("Received suppression data from controller: {}", suppressionData);
                suppressionData.getSuppressionFactor().forEach((topic, suppressionFactor) -> {
                    log.info("Updating suppression factor for topic: {} to {}", topic, suppressionFactor);
                    rateLimiterService.updateSuppressionFactor(topic, "throughput_check", suppressionFactor.getThroughputFactor());
                    rateLimiterService.updateSuppressionFactor(topic, "qps_check", suppressionFactor.getQpsFactor());
                });
            }
        });
    }

}
