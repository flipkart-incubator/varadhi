package com.flipkart.varadhi.spi.authn;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.entities.utils.RequestContext;
import io.vertx.core.Future;

public interface AuthenticationProvider {
    Future<Boolean> init(AuthenticationOptions authenticationOptions);

    Future<UserContext> authenticate(Org org, RequestContext requestContext);
}
