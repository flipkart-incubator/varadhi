package com.flipkart.varadhi.components;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.controller.ControllerMgr;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class Controller implements Component {

    private final ControllerMgr controllerMgr;

    public Controller(AppConfiguration configuration, CoreServices coreServices) {
        this.controllerMgr = new ControllerMgr(coreServices.getMessagingStackProvider(),
                coreServices.getMetaStoreProvider(),
                null,
                coreServices.getMeterRegistry());
    }
    @Override
    public Future<Void> start(Vertx vertx) {
        return controllerMgr.start();
    }

    @Override
    public Future<Void> shutdown(Vertx vertx) {
        return controllerMgr.shutdown();
    }
}
