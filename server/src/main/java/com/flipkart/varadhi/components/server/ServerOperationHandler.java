package com.flipkart.varadhi.components.server;

import com.flipkart.varadhi.ServerOpManager;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.concurrent.CompletableFuture;

public class ServerOperationHandler {
    private MetaStore metaStore;
    private ServerOpManager serverOpMgr;

    public ServerOperationHandler(ServerOpManager serverOpMgr, MetaStore metaStore) {
        this.serverOpMgr = serverOpMgr;
        this.metaStore = metaStore;
    }

    public CompletableFuture<Void> update(SubscriptionMessage message) {
        return serverOpMgr.update(null);
    }
}
