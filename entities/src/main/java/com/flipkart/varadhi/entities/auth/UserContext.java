package com.flipkart.varadhi.entities.auth;

import java.util.Map;

/*
 * UserContext represents the authenticated user. It is used to pass the user information across the system.
 * This can be enhanced to include more details like context around the authentication.
 */

public interface UserContext {

    String getSubject();

    boolean isExpired();

    default Map<String, String> getAttributes() {
        return Map.of();
    }
}
