package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.core.config.AppConfiguration;
import com.flipkart.varadhi.web.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.web.spi.authz.AuthorizationOptions;
import io.vertx.core.http.HttpServerOptions;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.util.List;

@Getter
public class WebConfiguration extends AppConfiguration {

    private List<String> disabledAPIs;

    @NotNull
    private HttpServerOptions httpServerOptions;

    private AuthenticationOptions authenticationOptions;

    private AuthorizationOptions authorizationOptions;

    @NotNull
    private RestOptions restOptions;
}
