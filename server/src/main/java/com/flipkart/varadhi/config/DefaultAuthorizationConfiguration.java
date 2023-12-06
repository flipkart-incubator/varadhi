package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.Role;
import lombok.Data;

import java.util.Map;

@Data
public class DefaultAuthorizationConfiguration {
    /**
     * role_id to Role{role_id, [permissions...]} mappings
     */
    private Map<String, Role> roleDefinitions;

    private String authZServerHost = "localhost";
    private int authZServerPort = 8080;
}
