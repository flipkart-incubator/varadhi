package com.flipkart.varadhi.auth;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ResourceNameTest {

    @Test
    public void testTemplateCreation() {
        ResourceName rt = new ResourceName("v1/{tenant}/{topic}.{user}/{auth}//");

        Map<String, String> env = new HashMap<>();
        env.put("tenant", "t1");
        env.put("topic", "topic_1");
        env.put("user", "user_1");
        env.put("auth", "auth_x");

        String resolved = rt.resolve(env::get);

        Assertions.assertEquals("v1/t1/topic_1.user_1/auth_x//", resolved);
    }
}
