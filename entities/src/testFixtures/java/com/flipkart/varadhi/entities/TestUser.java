package com.flipkart.varadhi.entities;

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
