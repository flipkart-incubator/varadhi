package com.flipkart.varadhi.config;

import com.flipkart.varadhi.server.spi.authn.AuthenticationOptions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;

@Data
@EqualsAndHashCode (callSuper = true)
public class AuthenticationConfig extends AuthenticationOptions {
    private String handlerProviderClassName;

    private List<String> whitelistedURLs;
}
