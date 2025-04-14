package com.flipkart.varadhi.spi.db;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, and subscriptions.
 */
public interface MetaStore {
    OrgMetaStore orgMetaStore();

    TeamMetaStore teamMetaStore();

    ProjectMetaStore projectMetaStore();

    TopicMetaStore topicMetaStore();

    SubscriptionMetaStore subscriptionMetaStore();

    boolean registerEventListener(MetaStoreEventListener listener);
}
