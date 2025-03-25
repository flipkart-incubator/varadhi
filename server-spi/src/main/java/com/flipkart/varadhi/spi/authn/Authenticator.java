package com.flipkart.varadhi.spi.authn;


import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.spi.RequestContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;

public interface Authenticator {
    Future<Boolean> init(AuthenticationOptions authenticationOptions, MeterRegistry meterRegistry);

    Future<UserContext> authenticate(String orgName, RequestContext requestContext);
}
