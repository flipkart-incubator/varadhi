package com.flipkart.varadhi.controller;


import com.flipkart.varadhi.exceptions.NotImplementedException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class ControllerMessageHandler  {
//public class ControllerMessageHandler implements MessageHandler {
//
//    private final Orchestrator orchestrator;
//
//    public ControllerMessageHandler(Orchestrator orchestrator) {
//        this.orchestrator = orchestrator;
//    }
//
//    @Override
//    public <E extends ClusterMessage> CompletableFuture<Void> handle(E message) {
//        log.info("Received message {} ", message);
//        if (message instanceof SubscriptionMessage) {
//            return orchestrator.handleSubscriptionMessage((SubscriptionMessage) message);
//        }else{
//            log.error("Unknown message type {}", message);
//            return CompletableFuture.failedFuture(new NotImplementedException("Unknown message type"));
//        }
//    }
//
//    @Override
//    public <E extends ClusterMessage> CompletableFuture<ResponseMessage> request(E message) {
//        log.error("Received message {} ", message);
//        return CompletableFuture.failedFuture(new NotImplementedException("Request not implemented"));
//    }
}
