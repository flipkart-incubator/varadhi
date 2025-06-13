package com.flipkart.varadhi.controller.config;

import com.flipkart.varadhi.core.config.AppConfiguration;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class ControllerConfiguration extends AppConfiguration {

    @NotNull
    private OperationsConfig operationsConfig;

    private EventProcessorConfig eventProcessorConfig;
}
