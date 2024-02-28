package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.MessageHandler;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;

public class ControllerMgr {
    public final String CONTROLLER_ADDRESS = "Controller" ;

    private final MetaStoreProvider metaStoreProvider;
    private final MessagingStackProvider messagingStackProvider;
    private final MeterRegistry meterRegistry;
    private final MessageHandler controllerHandler;
    private final Orchestrator orchestrator;

    private final MessageChannel channel;
    public ControllerMgr(MessagingStackProvider messagingStackProvider,
                         MetaStoreProvider metaStoreProvider,
                         MessageChannel channel,
                         MeterRegistry meterRegistry
                         ) {
        this.messagingStackProvider = messagingStackProvider;
        this.metaStoreProvider = metaStoreProvider;
        this.meterRegistry = meterRegistry;
        this.channel = channel;
        this.orchestrator = new Orchestrator();
        this.controllerHandler = new ControllerMessageHandler(this.orchestrator);
    }

    public Future<Void> start() {
        // leader election and on elected as leader

        // register message handler
//        channel.addMessageHandler(CONTROLLER_ADDRESS, controllerHandler);

        // initiate recovery
        return Future.succeededFuture();
    }

    public Future<Void> shutdown() {
        channel.removeMessageHandler(CONTROLLER_ADDRESS);
        return Future.succeededFuture();
    }
}
