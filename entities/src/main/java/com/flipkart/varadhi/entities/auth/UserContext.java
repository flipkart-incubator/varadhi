package com.flipkart.varadhi.entities.auth;

import java.util.Map;

public interface UserContext {

    String getSubject();

    boolean isExpired();

    String getOrg();

    Map<String, String> getAttributes();

    // TODO: enhance it to include more details like context around the authentication
}
