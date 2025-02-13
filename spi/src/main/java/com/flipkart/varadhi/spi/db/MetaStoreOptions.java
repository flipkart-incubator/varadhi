package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.spi.ConfigFile;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class MetaStoreOptions {

    @NotBlank private String providerClassName;

    @NotBlank @ConfigFile
    private String configFile;
}
