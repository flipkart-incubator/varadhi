package com.flipkart.varadhi.util;

import com.flipkart.varadhi.entities.UserContext;

public final class TestUtil {
    private TestUtil(){}

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
