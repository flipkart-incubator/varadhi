package com.flipkart.varadhi.pulsar.util;

/**
 * @author kaur.prabhpreet
 * On 22/12/23
 */

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EntityHelperTest {

    @Test
    void getNamespaceWithValidInputs() {
        String tenantName = "tenant1";
        String projectName = "project1";
        String expected = "tenant1/project1";

        String actual = EntityHelper.getNamespace(tenantName, projectName);

        assertEquals(expected, actual);
    }

    @Test
    void getNamespaceWithEmptyTenantName() {
        String tenantName = "";
        String projectName = "project1";
        String expected = "/project1";

        String actual = EntityHelper.getNamespace(tenantName, projectName);

        assertEquals(expected, actual);
    }

    @Test
    void getNamespaceWithEmptyProjectName() {
        String tenantName = "tenant1";
        String projectName = "";
        String expected = "tenant1/";

        String actual = EntityHelper.getNamespace(tenantName, projectName);

        assertEquals(expected, actual);
    }

    @Test
    void getNamespaceWithNullTenantName() {
        String tenantName = null;
        String projectName = "project1";
        String expected = "null/project1";

        String actual = EntityHelper.getNamespace(tenantName, projectName);

        assertEquals(expected, actual);
    }

    @Test
    void getNamespaceWithNullProjectName() {
        String tenantName = "tenant1";
        String projectName = null;
        String expected = "tenant1/null";

        String actual = EntityHelper.getNamespace(tenantName, projectName);

        assertEquals(expected, actual);
    }
}
