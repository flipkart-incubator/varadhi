package com.flipkart.varadhi.auth;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@AllArgsConstructor
public class DefaultAuthorizationConfiguration {
    private Map<String, List<ResourceAction>> roles;
    private Map<String, Map<String, List<String>>> roleBindings;
}
