package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.spi.utils.OrgResolver;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import static com.flipkart.varadhi.Constants.USER_ID_HEADER;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

@Slf4j
public class UserHeaderAuthenticationHandler implements AuthenticationHandlerProvider {

    @Override
    public AuthenticationHandler provideHandler(Vertx vertx, JsonObject jsonObject, OrgResolver orgResolver) {
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
