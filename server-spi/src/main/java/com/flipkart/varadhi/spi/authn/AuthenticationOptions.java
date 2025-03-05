package com.flipkart.varadhi.spi.authn;

import com.flipkart.varadhi.spi.ConfigFile;
import lombok.Data;

@Data
public class AuthenticationOptions {
    private String authenticatorClassName;

    @ConfigFile
    private String configFile;
}
