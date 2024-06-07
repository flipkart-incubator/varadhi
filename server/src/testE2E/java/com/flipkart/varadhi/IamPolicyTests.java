package com.flipkart.varadhi;

public class IamPolicyTests extends E2EBase {

    /**
     * Tests for iam policy, some cases to test are:
     * - initial superuser should have all permissions
     * - thanos creates flipkart org
     * - create admin user via set policy
     * - admin user should be able to create more user roles
     * - non admin users should not be able to create roles
     * - admin user can create team and project
     * - similarly for team and project scoped roles, do the same
     * - all the way down to topic level and produce level
     */
}
