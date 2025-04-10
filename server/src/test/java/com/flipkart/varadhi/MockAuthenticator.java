package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.server.spi.RequestContext;
import com.flipkart.varadhi.server.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.server.spi.authn.AuthenticationProvider;
import com.flipkart.varadhi.server.spi.utils.OrgResolver;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;

public class MockAuthenticator implements AuthenticationProvider {

    @Override
    public Future<Boolean> init(
        AuthenticationOptions authenticationOptions,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    ) {
        return Future.succeededFuture(true);
    }

    @Override
    public Future<UserContext> authenticate(String orgName, RequestContext requestContext) {
        return Future.succeededFuture(new UserContext() {
            @Override
            public String getSubject() {
                return "test.subject";
            }

            @Override
            public boolean isExpired() {
                return false;
            }
        });
    }
}
