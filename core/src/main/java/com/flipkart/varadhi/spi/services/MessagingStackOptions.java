package com.flipkart.varadhi.spi.services;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class MessagingStackOptions {

    @NotBlank
    private String providerClassName;

    @NotBlank
    private String configFile;
}
