package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;

import java.util.concurrent.CompletableFuture;

public interface TrafficSender {
    CompletableFuture<SuppressionData> send(ClientLoadInfo loadInfo);
}
