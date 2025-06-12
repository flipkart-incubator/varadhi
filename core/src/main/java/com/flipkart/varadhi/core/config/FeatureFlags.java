package com.flipkart.varadhi.core.config;

import com.flipkart.varadhi.common.Constants;
import lombok.Data;

@Data
public class FeatureFlags {
    private boolean leanDeployment;
    private String defaultOrg = Constants.RestDefaults.DEFAULT_ORG;
    private String defaultTeam = Constants.RestDefaults.DEFAULT_TEAM;
    private String defaultProject = Constants.RestDefaults.DEFAULT_PROJECT;
}
