package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.spi.utils.OrgResolver;
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

@Slf4j
public class UserHeaderAuthenticationHandler implements AuthenticationHandlerProvider {

    /**
     * Provides an authentication handler that validates requests based on a user header.
     * This handler expects the user ID to be present in the USER_ID_HEADER of incoming requests.
     *
     * @param vertx       The Vertx instance
     * @param jsonObject  Configuration parameters (not used for header-based auth)
     * @param orgResolver Organization resolver (not used for header-based auth)
     * @return An AuthenticationHandler that validates the user header and creates a User object
     * @throws HttpException with 401 status if the user header is missing or empty
     */

    @Override
    public AuthenticationHandler provideHandler(
        Vertx vertx,
        JsonObject jsonObject,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    ) {
        log.warn("Staring to configure User header based authentication.");

        return SimpleAuthenticationHandler.create().authenticate(ctx -> {
            String userName = ctx.request().getHeader(USER_ID_HEADER);
            if (StringUtils.isBlank(userName)) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "no user details present"));
            }
            return Future.succeededFuture(User.fromName(userName));
        });
    }
}
