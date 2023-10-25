package com.flipkart.varadhi.auth;

import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class DefaultAuthorizationConfiguration {
    private Map<String, Set<String>> roles;
    private Map<String, Map<String, Set<String>>> roleBindings;
}
