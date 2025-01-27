package com.flipkart.varadhi.spi.authn;

import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.AuthenticationHandler;

public interface AuthenticationHandlerProvider {
    AuthenticationHandler provideHandler(Vertx vertx, String fileName);
}
