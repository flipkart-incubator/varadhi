package com.flipkart.varadhi.entities.auth;

import java.util.Map;
import java.util.Optional;

public interface UserContext {

    String getSubject();

    boolean isExpired();

    default Optional<String> getOrg() {
        return Optional.empty();
    }

    default Map<String, String> getAttributes() {
        return Map.of();
    }

    // TODO: enhance it to include more details like context around the authentication
}
