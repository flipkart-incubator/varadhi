package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.spi.utils.OrgResolver;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AnonymousAuthenticationHandler implements AuthenticationHandlerProvider {

    /**
     * Provides an authentication handler that allows anonymous access.
     * SECURITY WARNING: This handler grants access without authentication.
     * Use only in controlled environments where anonymous access is explicitly required.
     *
     * @param vertx                Vertx instance
     * @param authenticationConfig Configuration for authentication
     * @return AuthenticationHandler that allows anonymous access
     */

    @Override
    public AuthenticationHandler provideHandler(Vertx vertx, JsonObject jsonObject, OrgResolver orgResolver) {
        log.warn("Anonymous authentication is enabled. This allows unauthenticated access.");

        return SimpleAuthenticationHandler.create().authenticate(ctx -> {
            log.info("Anonymous access attempt from: {}", ctx.request().remoteAddress());
            return Future.succeededFuture(User.fromName("anonymous"));
        });
    }
}
