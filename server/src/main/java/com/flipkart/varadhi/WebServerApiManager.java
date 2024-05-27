package com.flipkart.varadhi;


import com.flipkart.varadhi.core.cluster.OperationMgr;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.core.cluster.WebServerApi;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class WebServerApiManager implements WebServerApi {
    private final OperationMgr operationMgr;

    public WebServerApiManager(OperationMgr operationMgr) {
        this.operationMgr = operationMgr;
    }

    @Override
    public CompletableFuture<Void> update(SubscriptionOperation.OpData operation) {
        log.debug("Received update on subscription: {}", operation);
        operationMgr.updateSubOp(operation);
        //TODO::Fix this.
        return CompletableFuture.completedFuture(null);
    }
}
