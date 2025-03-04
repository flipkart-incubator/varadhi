package com.flipkart.varadhi.config;

import com.flipkart.varadhi.spi.authn.AuthenticationOptions;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode (callSuper = true)
public class AuthenticationConfig extends AuthenticationOptions {
    private String handlerProviderClassName;
}
