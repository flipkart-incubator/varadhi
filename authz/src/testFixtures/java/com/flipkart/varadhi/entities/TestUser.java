package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.UserContext;

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
        };
    }
}
