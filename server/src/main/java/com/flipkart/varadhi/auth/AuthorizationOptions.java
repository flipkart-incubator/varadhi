package com.flipkart.varadhi.auth;

import lombok.Data;

import java.util.List;

@Data
public class AuthorizationOptions {

    private List<String> superUsers;

    /**
     * Fully qualified package path to the class implementing AuthorizationProvider interface.<br/>
     * If {@link AuthorizationOptions#useDefaultProvider} is {@code true}, then this configuration is ignored.
     */
    private String providerClassName;

    /**
     * Path to a file having authorization provider configs.<br>
     * This file can be in any format, since the provider will implement the logic to parse this config file.<br>
     */
    private String configFile;

    /**
     * If specified as true, then the default implementation via {@link com.flipkart.varadhi.auth.DefaultAuthorizationProvider} is used.<br/>
     * {@link AuthorizationOptions#providerClassName} value is ignored.
     */
    private Boolean useDefaultProvider;
}
