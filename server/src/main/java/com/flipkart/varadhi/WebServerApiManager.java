package com.flipkart.varadhi;


import com.flipkart.varadhi.core.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class WebServerApiManager implements WebServerApi {
    @Override
    public CompletableFuture<Void> update(SubscriptionOperation.OpData operation) {
        log.debug("Received update on subscription: {}", operation);
        return CompletableFuture.completedFuture(null);
    }
}
