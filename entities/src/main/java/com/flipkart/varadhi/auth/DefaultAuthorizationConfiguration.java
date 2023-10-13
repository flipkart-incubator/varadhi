package com.flipkart.varadhi.auth;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DefaultAuthorizationConfiguration {
    private Map<String, List<String>> roles;
    private Map<String, Map<String, List<String>>> roleBindings;
}
