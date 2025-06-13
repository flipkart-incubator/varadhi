package com.flipkart.varadhi.controller.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.varadhi.core.config.AppConfiguration;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
@JsonIgnoreProperties (ignoreUnknown = true)
public class ControllerConfiguration extends AppConfiguration {

    @NotNull
    private OperationsConfig operationsConfig;

    private EventProcessorConfig eventProcessorConfig;
}
