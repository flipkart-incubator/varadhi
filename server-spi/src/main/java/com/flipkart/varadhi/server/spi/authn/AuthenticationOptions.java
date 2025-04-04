package com.flipkart.varadhi.server.spi.authn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.varadhi.server.spi.vo.URLDefinition;
import com.flipkart.varadhi.spi.ConfigFile;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties (ignoreUnknown = true)
public class AuthenticationOptions {
    private String handlerProviderClassName;
    private List<URLDefinition> whitelistedURLs;
    private List<URLDefinition> orgContextExemptionURLs;
    private String authenticationProviderClassName;

    @ConfigFile
    private String configFile;
}
