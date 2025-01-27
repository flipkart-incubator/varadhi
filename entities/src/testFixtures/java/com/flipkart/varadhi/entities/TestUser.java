package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.UserContext;

import java.util.Map;

public final class TestUser {
    private TestUser() {
    }

    public static UserContext testUser(String name, boolean expired) {
        return new UserContext() {
            @Override
            public String getSubject() {
                return name;
            }

            @Override
            public boolean isExpired() {
                return expired;
            }

            @Override
            public String getOrg() {
                return "";
            }

            @Override
            public Map<String, String> getAttributes() {
                return Map.of();
            }
        };
    }
}
