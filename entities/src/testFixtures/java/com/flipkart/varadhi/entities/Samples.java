package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;

public class Samples {

    public static final Project PROJECT_1 = Project.of("project1", "", "team1", "org1");
    public static final TopicResource U_TOPIC_RESOURCE_1 = TopicResource.unGrouped(
        "topic1",
        "project1",
        null,
        LifecycleStatus.ActionCode.SYSTEM_ACTION,
        "test"
    );
    public static final SubscriptionResource U_SUB_RESOURCE_1 = SubscriptionTestUtils.defaultSubscriptionResource(
        PROJECT_1,
        U_TOPIC_RESOURCE_1,
        "SUB_1"
    );
}
