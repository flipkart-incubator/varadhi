package com.flipkart.varadhi.entities.auth;

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
    ORG_CREATE(EntityType.ORG, Action.CREATE), ORG_UPDATE(EntityType.ORG, Action.UPDATE), ORG_DELETE(
        EntityType.ORG,
        Action.DELETE
    ), ORG_GET(EntityType.ORG, Action.GET), ORG_LIST(EntityType.ROOT, Action.LIST), ORG_PROJECT_MIGRATE(
        EntityType.ORG,
        Action.MIGRATE
    ),

    /**
     * Actions related to team resources.
     */
    TEAM_CREATE(EntityType.TEAM, Action.CREATE), TEAM_UPDATE(EntityType.TEAM, Action.UPDATE), TEAM_DELETE(
        EntityType.TEAM,
        Action.DELETE
    ), TEAM_GET(EntityType.TEAM, Action.GET), TEAM_LIST(EntityType.ORG, Action.LIST),

    /**
     * Actions related to project resources.
     */
    PROJECT_CREATE(EntityType.PROJECT, Action.CREATE), PROJECT_UPDATE(
        EntityType.PROJECT,
        Action.UPDATE
    ), PROJECT_DELETE(EntityType.PROJECT, Action.DELETE), PROJECT_GET(EntityType.PROJECT, Action.GET), PROJECT_LIST(
        EntityType.TEAM,
        Action.LIST
    ),

    /**
     * Actions related to topic resources.
     */
    TOPIC_CREATE(EntityType.TOPIC, Action.CREATE), TOPIC_UPDATE(EntityType.TOPIC, Action.UPDATE), TOPIC_DELETE(
        EntityType.TOPIC,
        Action.DELETE
    ), TOPIC_GET(EntityType.TOPIC, Action.GET), TOPIC_LIST(EntityType.PROJECT, Action.LIST), TOPIC_SUBSCRIBE(
        EntityType.TOPIC,
        Action.SUBSCRIBE
    ), TOPIC_PRODUCE(EntityType.TOPIC, Action.PRODUCE),

    /**
     * Actions related to subscription resources.
     */
    SUBSCRIPTION_CREATE(EntityType.SUBSCRIPTION, Action.CREATE), SUBSCRIPTION_UPDATE(
        EntityType.SUBSCRIPTION,
        Action.UPDATE
    ), SUBSCRIPTION_DELETE(EntityType.SUBSCRIPTION, Action.DELETE), SUBSCRIPTION_GET(
        EntityType.SUBSCRIPTION,
        Action.GET
    ), SUBSCRIPTION_LIST(EntityType.PROJECT, Action.LIST), SUBSCRIPTION_SEEK(EntityType.SUBSCRIPTION, Action.SEEK),

    /**
     * Actions related to IAM policy resources.
     */
    IAM_POLICY_GET(EntityType.IAM_POLICY, Action.GET), IAM_POLICY_SET(
        EntityType.IAM_POLICY,
        Action.SET
    ), IAM_POLICY_DELETE(EntityType.IAM_POLICY, Action.DELETE);

    /**
     * The type of resource associated with the action.
     */
    private final EntityType entityType;

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
        return String.format("%s.%s", entityType, action);
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
