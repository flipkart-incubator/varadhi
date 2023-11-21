package com.flipkart.varadhi.config;

import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class DefaultAuthorizationConfiguration {
    private Map<String, Set<String>> roles;
    private Map<String, Map<String, Set<String>>> roleBindings;
}
