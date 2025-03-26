package com.flipkart.varadhi.spi.authn;

import com.flipkart.varadhi.spi.utils.OrgResolver;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.AuthenticationHandler;

public interface AuthenticationHandlerProvider {
    AuthenticationHandler provideHandler(
        Vertx vertx,
        JsonObject jsonObject,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    );
}
