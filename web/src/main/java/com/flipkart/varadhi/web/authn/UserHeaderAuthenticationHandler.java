package com.flipkart.varadhi.web.authn;

import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.web.spi.RequestContext;
import com.flipkart.varadhi.web.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.web.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.web.spi.authn.AuthenticationProvider;
import com.flipkart.varadhi.web.spi.utils.OrgResolver;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import static com.flipkart.varadhi.common.Constants.USER_ID_HEADER;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import java.net.URISyntaxException;

@Slf4j
public class UserHeaderAuthenticationHandler implements AuthenticationHandlerProvider {

    /**
     * Provides an authentication handler that validates requests based on a user header.
     * This handler expects the user ID to be present in the USER_ID_HEADER of incoming requests.
     *
     * @param vertx       The Vertx instance
     * @param configObject  Configuration parameters (not used for header-based auth)
     * @param orgResolver Organization resolver (not used for header-based auth)
     * @return An AuthenticationHandler that validates the user header and creates a User object
     * @throws HttpException with 401 status if the user header is missing or empty
     */

    @Override
    public AuthenticationHandler provideHandler(
        Vertx vertx,
        JsonObject configObject,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    ) {
        log.warn("Staring to configure User header based authentication.");

        AuthnProvider authnProvider = new AuthnProvider();
        return SimpleAuthenticationHandler.create().authenticate(ctx -> {
            RequestContext requestContext;
            try {
                requestContext = new RequestContext(ctx);
            } catch (URISyntaxException e) {
                return Future.failedFuture(e);
            }
            return authnProvider.authenticate(null, requestContext).map(userContext -> {
                return User.fromName(userContext.getSubject());
            });
        });
    }

    public static class AuthnProvider implements AuthenticationProvider {

        @Override
        public Future<Boolean> init(
            AuthenticationOptions authenticationOptions,
            OrgResolver orgResolver,
            MeterRegistry meterRegistry
        ) {
            return Future.succeededFuture(true);
        }

        @Override
        public Future<UserContext> authenticate(String orgName, RequestContext ctx) {
            String userName = ctx.getHeaders().get(USER_ID_HEADER);
            if (StringUtils.isBlank(userName)) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "no user details present"));
            }
            return Future.succeededFuture(new UserContext() {
                @Override
                public String getSubject() {
                    return userName;
                }

                @Override
                public boolean isExpired() {
                    return false;
                }
            });
        }
    }
}
