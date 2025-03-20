package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.spi.db.org.OrgFilterOperations;
import com.flipkart.varadhi.spi.db.org.OrgOperations;
import com.flipkart.varadhi.spi.db.project.ProjectOperations;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionOperations;
import com.flipkart.varadhi.spi.db.team.TeamOperations;
import com.flipkart.varadhi.spi.db.topic.TopicOperations;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, and subscriptions.
 */
public interface MetaStore {
    OrgOperations orgOperations();
    OrgFilterOperations orgLevelFilters();

    TeamOperations teamOperations();

    ProjectOperations projectOperations();

    TopicOperations topicOperations();

    SubscriptionOperations subscriptionOperations();

    boolean registerEventListener(MetaStoreEventListener listener);
}
