package com.flipkart.varadhi.spi.db;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, and subscriptions.
 */
public interface MetaStore {
    OrgStore orgMetaStore();

    TeamStore teamMetaStore();

    ProjectStore projectMetaStore();

    TopicStore topicMetaStore();

    SubscriptionStore subscriptionMetaStore();

    boolean registerEventListener(MetaStoreEventListener listener);
}
