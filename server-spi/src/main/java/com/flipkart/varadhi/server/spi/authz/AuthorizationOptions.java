package com.flipkart.varadhi.server.spi.authz;

import com.flipkart.varadhi.spi.ConfigFile;
import lombok.Data;


@Data
public class AuthorizationOptions {

    private boolean enabled;

    /**
     * Fully qualified package path to the class implementing AuthorizationProvider interface.
     */
    private String providerClassName;

    /**
     * Path to a file having authorization provider configs.<br>
     * This file can be in any format, since the provider will implement the logic to parse this config file.<br>
     */
    @ConfigFile
    private String configFile;
}
