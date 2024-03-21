package com.flipkart.varadhi;


import com.flipkart.varadhi.core.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;

import java.util.concurrent.CompletableFuture;

public class WebServerApiManager implements WebServerApi {
    @Override
    public CompletableFuture<Void> update(SubscriptionOperation.OpData operation) {
        return CompletableFuture.completedFuture(null);
    }
}
