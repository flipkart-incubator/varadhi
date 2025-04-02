package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.spi.db.IamPolicy.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.org.OrgMetaStore;
import com.flipkart.varadhi.spi.db.project.ProjectMetaStore;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionMetaStore;
import com.flipkart.varadhi.spi.db.team.TeamMetaStore;
import com.flipkart.varadhi.spi.db.topic.TopicMetaStore;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, and subscriptions.
 */
public interface MetaStore {
    OrgMetaStore orgOperations();

    TeamMetaStore teamOperations();

    ProjectMetaStore projectOperations();

    TopicMetaStore topicOperations();

    SubscriptionMetaStore subscriptionOperations();

    IamPolicyMetaStore iamPolicyOperations();

    boolean registerEventListener(MetaStoreEventListener listener);
}
