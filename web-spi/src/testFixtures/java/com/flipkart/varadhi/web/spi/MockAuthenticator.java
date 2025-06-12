package com.flipkart.varadhi.web.spi;

import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.web.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.web.spi.authn.AuthenticationProvider;
import com.flipkart.varadhi.web.spi.utils.OrgResolver;
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
