package com.flipkart.varadhi.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.flipkart.varadhi.authn.AuthenticationMechanism;
import com.flipkart.varadhi.authn.AuthenticationMechanismDeserializer;
import com.flipkart.varadhi.spi.authn.AuthenticationOptions;
import lombok.Data;

@Data
public class AuthenticationConfig extends AuthenticationOptions {

    @JsonDeserialize (using = AuthenticationMechanismDeserializer.class)
    private AuthenticationMechanism mechanism;
}
