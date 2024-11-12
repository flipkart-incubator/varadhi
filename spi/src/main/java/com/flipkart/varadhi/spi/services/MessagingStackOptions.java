package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.spi.ConfigFile;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class MessagingStackOptions {

    @NotBlank
    private String providerClassName;

    @NotBlank
    @ConfigFile
    private String configFile;
}
