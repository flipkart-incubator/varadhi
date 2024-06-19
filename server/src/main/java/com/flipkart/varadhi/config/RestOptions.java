package com.flipkart.varadhi.config;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import static com.flipkart.varadhi.Constants.REST_DEFAULTS.*;

@Data
public class RestOptions {
    @NotBlank
    private String deployedRegion;
    @NotNull
    private String projectCacheBuilderSpec = "expireAfterWrite=3600s";
    private TopicCapacityPolicy defaultTopicCapacity = Constants.DefaultTopicCapacity;
    private boolean traceRequestEnabled = true;
    private int payloadSizeMax = PAYLOAD_SIZE_MAX;
    private int headersAllowedMax = HEADERS_ALLOWED_MAX;
    private int headerNameSizeMax = HEADER_NAME_SIZE_MAX;
    private int headerValueSizeMax = HEADER_VALUE_SIZE_MAX;

    // TODO: These dont look related to rest. Looks related to lean deployment.
    private String defaultOrg = DEFAULT_ORG;
    private String defaultTeam = DEFAULT_TEAM;
    private String defaultProject = DEFAULT_PROJECT;
}
