package com.flipkart.varadhi.config;

import com.flipkart.varadhi.authn.AuthenticationMechanism;
import com.flipkart.varadhi.spi.authn.AuthenticationOptions;
import lombok.Data;

@Data
public class AuthenticationConfig extends AuthenticationOptions {
    private AuthenticationMechanism mechanism;
}
