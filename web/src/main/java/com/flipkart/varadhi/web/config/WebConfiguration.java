package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.core.config.AppConfiguration;
import com.flipkart.varadhi.web.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.web.spi.authz.AuthorizationOptions;
import io.vertx.core.http.HttpServerOptions;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class WebConfiguration extends AppConfiguration {

    private List<String> disabledAPIs;

    @NotNull
    private HttpServerOptions httpServerOptions;

    private AuthenticationOptions authenticationOptions;

    private AuthorizationOptions authorization;

    @NotNull
    private RestOptions restOptions;
}
