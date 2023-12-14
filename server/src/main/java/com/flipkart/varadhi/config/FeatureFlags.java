package com.flipkart.varadhi.config;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class FeatureFlags {

    @NotNull
    private boolean leanDeployment;
}
