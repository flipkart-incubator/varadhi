package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class RestOptions {

    @NotBlank (message = "Deployed region must be specified")
    private String deployedRegion;
    private TopicCapacityPolicy defaultTopicCapacity = Constants.DEFAULT_TOPIC_CAPACITY;
    private boolean traceRequestEnabled = true;
    private int payloadSizeMax = Constants.RestDefaults.PAYLOAD_SIZE_MAX;

    private int unsidelineApiMsgCountMax = 1000;
    private int unsidelineApiGroupCountMax = 100;
    private int unsidelineApiMsgCountDefault = 100;
    private int unsidelineApiGroupCountDefault = 20;

    private int getMessagesApiMessagesLimitMax = 1000;
    private int getMessagesApiMessagesLimitDefault = 100;

    // TODO: These dont look related to rest. Looks related to lean deployment.
    private String defaultOrg = Constants.RestDefaults.DEFAULT_ORG;
    private String defaultTeam = Constants.RestDefaults.DEFAULT_TEAM;
    private String defaultProject = Constants.RestDefaults.DEFAULT_PROJECT;
}
