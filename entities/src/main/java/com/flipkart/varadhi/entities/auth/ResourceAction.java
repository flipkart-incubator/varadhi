package com.flipkart.varadhi.entities.auth;

import com.flipkart.varadhi.entities.ResourceType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Enum representing various resource actions in the system.
 */
@Getter
@RequiredArgsConstructor
public enum ResourceAction {

    /**
     * Actions related to organization resources.
     */
    ORG_CREATE(ResourceType.ORG, Action.CREATE), ORG_UPDATE(ResourceType.ORG, Action.UPDATE), ORG_DELETE(
        ResourceType.ORG,
        Action.DELETE
    ), ORG_GET(ResourceType.ORG, Action.GET), ORG_LIST(ResourceType.ROOT, Action.LIST), ORG_PROJECT_MIGRATE(
        ResourceType.ORG,
        Action.MIGRATE
    ),

    /**
     * Actions related to team resources.
     */
    TEAM_CREATE(ResourceType.TEAM, Action.CREATE), TEAM_UPDATE(ResourceType.TEAM, Action.UPDATE), TEAM_DELETE(
        ResourceType.TEAM,
        Action.DELETE
    ), TEAM_GET(ResourceType.TEAM, Action.GET), TEAM_LIST(ResourceType.ORG, Action.LIST),

    /**
     * Actions related to project resources.
     */
    PROJECT_CREATE(ResourceType.PROJECT, Action.CREATE), PROJECT_UPDATE(
        ResourceType.PROJECT,
        Action.UPDATE
    ), PROJECT_DELETE(ResourceType.PROJECT, Action.DELETE), PROJECT_GET(ResourceType.PROJECT, Action.GET), PROJECT_LIST(
        ResourceType.TEAM,
        Action.LIST
    ),

    /**
     * Actions related to topic resources.
     */
    TOPIC_CREATE(ResourceType.TOPIC, Action.CREATE), TOPIC_UPDATE(ResourceType.TOPIC, Action.UPDATE), TOPIC_DELETE(
        ResourceType.TOPIC,
        Action.DELETE
    ), TOPIC_GET(ResourceType.TOPIC, Action.GET), TOPIC_LIST(ResourceType.PROJECT, Action.LIST), TOPIC_SUBSCRIBE(
        ResourceType.TOPIC,
        Action.SUBSCRIBE
    ), TOPIC_PRODUCE(ResourceType.TOPIC, Action.PRODUCE),

    /**
     * Actions related to subscription resources.
     */
    SUBSCRIPTION_CREATE(ResourceType.SUBSCRIPTION, Action.CREATE), SUBSCRIPTION_UPDATE(
        ResourceType.SUBSCRIPTION,
        Action.UPDATE
    ), SUBSCRIPTION_DELETE(ResourceType.SUBSCRIPTION, Action.DELETE), SUBSCRIPTION_GET(
        ResourceType.SUBSCRIPTION,
        Action.GET
    ), SUBSCRIPTION_LIST(ResourceType.PROJECT, Action.LIST), SUBSCRIPTION_SEEK(ResourceType.SUBSCRIPTION, Action.SEEK),

    /**
     * Actions related to IAM policy resources.
     */
    IAM_POLICY_GET(ResourceType.IAM_POLICY, Action.GET), IAM_POLICY_SET(
        ResourceType.IAM_POLICY,
        Action.SET
    ), IAM_POLICY_DELETE(ResourceType.IAM_POLICY, Action.DELETE);

    /**
     * The type of resource associated with the action.
     */
    private final ResourceType resourceType;

    /**
     * The action to be performed on the resource.
     */
    private final Action action;

    /**
     * Returns the string representation of the resource action.
     *
     * @return the string representation of the resource action
     */
    @Override
    public String toString() {
        return String.format("varadhi.%s.%s", resourceType.toString(), action);
    }

    /**
     * Enum representing all possible actions on resources.
     */
    @Getter
    @RequiredArgsConstructor
    public enum Action {
        CREATE("create"), UPDATE("update"), DELETE("delete"), GET("get"), LIST("list"), MIGRATE("migrate"), PRODUCE(
            "produce"
        ), SEEK("seek"), SET("set"), SUBSCRIBE("subscribe");

        /**
         * The name of the action.
         */
        private final String actionName;

        @Override
        public String toString() {
            return actionName;
        }
    }
}
