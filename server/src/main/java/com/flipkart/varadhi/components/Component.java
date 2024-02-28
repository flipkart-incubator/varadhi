package com.flipkart.varadhi.components;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.config.AppConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public interface Component {
    Future<Void> start(Vertx vertx);
    Future<Void> shutdown(Vertx vertx);
}
