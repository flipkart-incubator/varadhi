package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.server.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.server.spi.utils.OrgResolver;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.web.Extensions.ANONYMOUS_IDENTITY;

@Slf4j
public class AnonymousAuthenticationHandler implements AuthenticationHandlerProvider {

    /**
     * Provides an authentication handler that allows anonymous access.
     * This handler should only be used in development or testing environments.
     *
     * @param vertx       The Vertx instance
     * @param configObject  Configuration parameters (not used for anonymous auth)
     * @param orgResolver Organization resolver (not used for anonymous auth)
     * @return An AuthenticationHandler that allows all requests with an anonymous user
     */

    @Override
    public AuthenticationHandler provideHandler(
        Vertx vertx,
        JsonObject configObject,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    ) {
        log.warn("Anonymous authentication is enabled. This allows unauthenticated access.");

        return SimpleAuthenticationHandler.create().authenticate(ctx -> {
            log.info("Anonymous access attempt from: {}", ctx.request().remoteAddress());
            return Future.succeededFuture(User.fromName(ANONYMOUS_IDENTITY));
        });
    }
}
