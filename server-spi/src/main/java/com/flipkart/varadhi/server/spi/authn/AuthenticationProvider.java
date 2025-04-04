package com.flipkart.varadhi.server.spi.authn;


import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.server.spi.RequestContext;
import com.flipkart.varadhi.server.spi.utils.OrgResolver;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;

public interface AuthenticationProvider {
    Future<Boolean> init(
        AuthenticationOptions authenticationOptions,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    );

    Future<UserContext> authenticate(String orgName, RequestContext requestContext);
}
