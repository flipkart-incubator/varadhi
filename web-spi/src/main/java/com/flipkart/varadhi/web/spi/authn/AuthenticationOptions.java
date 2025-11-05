package com.flipkart.varadhi.web.spi.authn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.varadhi.web.spi.vo.URLDefinition;
import com.flipkart.varadhi.spi.ConfigFile;
import lombok.Data;

import java.util.List;

/**
 * Authentication Handler hooks into the http router's handler chain. Usually the handler
 * requires a authenticator to validate the credentials present in the request.
 */

@Data
@JsonIgnoreProperties (ignoreUnknown = true)
public class AuthenticationOptions {

    /**
     * The handler provider class name that implements AuthenticationHandlerProvider interface.
     */
    private String handlerProviderClassName;
    private List<URLDefinition> orgContextExemptionURLs;

    /**
     * Optional. The authenticator provider class name that implements AuthenticationProvider interface.
     */
    private String authenticationProviderClassName;

    /**
     * Optional. Any custom configuration file for the above classes.
     */
    @ConfigFile
    private String configFile;
}
