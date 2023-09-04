package com.flipkart.varadhi.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
public class DefaultAuthorizationConfiguration {
    private final Map<String, List<ResourceAction>> roles;
    private final Map<String, Map<String, List<String>>> roleBindings;

    @JsonCreator
    public DefaultAuthorizationConfiguration(@JsonProperty("roles") Map<String, List<ResourceAction>> roles,
                                             @JsonProperty("roleBindings") Map<String, Map<String, List<String>>> roleBindings) {
        this.roles = roles;
        this.roleBindings = roleBindings;
    }
}
