package com.flipkart.varadhi.entities.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ResourceActionTest {

    @Test
    void toString_ReturnsCorrectFormat() {
        assertAll(
            () -> assertEquals("varadhi.org.create", ResourceAction.ORG_CREATE.toString()),
            () -> assertEquals("varadhi.team.update", ResourceAction.TEAM_UPDATE.toString()),
            () -> assertEquals("varadhi.project.delete", ResourceAction.PROJECT_DELETE.toString()),
            () -> assertEquals("varadhi.topic.get", ResourceAction.TOPIC_GET.toString()),
            () -> assertEquals("varadhi.project.list", ResourceAction.SUBSCRIPTION_LIST.toString()),
            () -> assertEquals("varadhi.iam_policy.set", ResourceAction.IAM_POLICY_SET.toString())
        );
    }

    @Test
    void actionName_ReturnsCorrectActionName() {
        assertAll(
            () -> assertEquals("create", ResourceAction.Action.CREATE.toString()),
            () -> assertEquals("update", ResourceAction.Action.UPDATE.toString()),
            () -> assertEquals("delete", ResourceAction.Action.DELETE.toString()),
            () -> assertEquals("get", ResourceAction.Action.GET.toString()),
            () -> assertEquals("list", ResourceAction.Action.LIST.toString()),
            () -> assertEquals("migrate", ResourceAction.Action.MIGRATE.toString()),
            () -> assertEquals("subscribe", ResourceAction.Action.SUBSCRIBE.toString()),
            () -> assertEquals("produce", ResourceAction.Action.PRODUCE.toString()),
            () -> assertEquals("seek", ResourceAction.Action.SEEK.toString()),
            () -> assertEquals("set", ResourceAction.Action.SET.toString())
        );
    }

    @Test
    void resourceType_ReturnsCorrectResourceType() {
        assertAll(
            () -> assertEquals(EntityType.ORG, ResourceAction.ORG_CREATE.getEntityType()),
            () -> assertEquals(EntityType.TEAM, ResourceAction.TEAM_UPDATE.getEntityType()),
            () -> assertEquals(EntityType.PROJECT, ResourceAction.PROJECT_DELETE.getEntityType()),
            () -> assertEquals(EntityType.TOPIC, ResourceAction.TOPIC_GET.getEntityType()),
            () -> assertEquals(EntityType.PROJECT, ResourceAction.SUBSCRIPTION_LIST.getEntityType()),
            () -> assertEquals(EntityType.IAM_POLICY, ResourceAction.IAM_POLICY_SET.getEntityType())
        );
    }

    @Test
    void action_ReturnsCorrectAction() {
        assertAll(
            () -> assertEquals(ResourceAction.Action.CREATE, ResourceAction.ORG_CREATE.getAction()),
            () -> assertEquals(ResourceAction.Action.UPDATE, ResourceAction.TEAM_UPDATE.getAction()),
            () -> assertEquals(ResourceAction.Action.DELETE, ResourceAction.PROJECT_DELETE.getAction()),
            () -> assertEquals(ResourceAction.Action.GET, ResourceAction.TOPIC_GET.getAction()),
            () -> assertEquals(ResourceAction.Action.LIST, ResourceAction.SUBSCRIPTION_LIST.getAction()),
            () -> assertEquals(ResourceAction.Action.SET, ResourceAction.IAM_POLICY_SET.getAction())
        );
    }
}
