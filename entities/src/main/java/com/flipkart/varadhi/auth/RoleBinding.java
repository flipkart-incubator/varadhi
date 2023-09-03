package com.flipkart.varadhi.auth;

import java.util.List;
import java.util.Map;

public record RoleBinding(String resourceId, Map<Role, List<String>> roleBindings){}
