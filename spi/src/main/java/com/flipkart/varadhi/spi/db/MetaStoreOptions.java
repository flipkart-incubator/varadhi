package com.flipkart.varadhi.spi.db;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class MetaStoreOptions {

    @NotBlank
    private String providerClassName;

    @NotBlank
    private String configFile;
}
