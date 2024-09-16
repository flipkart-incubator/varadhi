package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.ratelimit.LoadInfo;
import com.flipkart.varadhi.entities.ratelimit.SuppressionData;
import com.flipkart.varadhi.qos.RLObserver;
import com.flipkart.varadhi.qos.RateLimiter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrafficDataHandler implements RLObserver {

    private final MessageExchange exchange;
    private final RateLimiter rateLimiter;

    public TrafficDataHandler(MessageExchange exchange, RateLimiter rateLimiter) {
        this.exchange = exchange;
        this.rateLimiter = rateLimiter;
        rateLimiter.registerObserver(this);
    }

    public void handle(ClusterMessage message) {
//        System.out.println("msg: " + message);
        LoadInfo info = message.getData(LoadInfo.class);
//        System.out.println(info.getTopicUsageMap());
//        System.out.println(new Date(info.getFrom()));
//        System.out.println(new Date(info.getTo()));
        System.out.println("Delta: " + (System.currentTimeMillis() - info.getTo()) + "ms");
        rateLimiter.addTrafficData(info);

    }

    private void sendSuppressionFactor(SuppressionData suppressionData) {
        log.info("Sending factor to producers");
        ClusterMessage msg = ClusterMessage.of(suppressionData);
        exchange.publish("web", "rate-limit", msg);
    }

    @Override
    public void update(SuppressionData suppressionData) {
        sendSuppressionFactor(suppressionData);
    }

}
