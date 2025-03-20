package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.db.org.OrgOperations;
import com.flipkart.varadhi.spi.db.project.ProjectOperations;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionOperations;
import com.flipkart.varadhi.spi.db.team.TeamOperations;
import com.flipkart.varadhi.spi.db.topic.TopicOperations;

import java.util.List;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, and subscriptions.
 */
public interface MetaStore {
    OrgOperations orgOperations();

    TeamOperations teamOperations();

    ProjectOperations projectOperations();

    TopicOperations topicOperations();

    SubscriptionOperations subscriptionOperations();

    boolean registerEventListener(MetaStoreEventListener listener);
}
