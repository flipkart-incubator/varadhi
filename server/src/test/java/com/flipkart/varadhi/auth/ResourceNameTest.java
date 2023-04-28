package com.flipkart.varadhi.auth;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ResourceNameTest {

    @Test
    public void testTemplateCreation() {
        Map<String, String> env = new HashMap<>();
        env.put("tenant", "t1");
        env.put("topic", "topic_1");
        env.put("user", "user_1");
        env.put("auth", "auth_x");

        ResourceName rt = new ResourceName("v1/{tenant}/{topic}.{user}/{auth}//");
        Assertions.assertEquals("v1/t1/topic_1.user_1/auth_x//", rt.resolve(env::get));

        ResourceName noVariable = new ResourceName("hello_world");
        Assertions.assertEquals("hello_world", noVariable.resolve(env::get));

        ResourceName simple = new ResourceName("{user}");
        Assertions.assertEquals("user_1", simple.resolve(env::get));
    }

    @Test
    public void testResourceTemplateNotDefined() {
        ResourceName notPresent = new ResourceName("{nothing}");
        Map<String, String> env = new HashMap<>();
        Assertions.assertThrows(IllegalStateException.class, () -> notPresent.resolve(env::get));
    }
}
