package com.flipkart.varadhi.spi.db;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, and subscriptions.
 */
public interface MetaStore {

    OrgStore orgs();

    TeamStore teams();

    ProjectStore projects();

    TopicStore topics();

    SubscriptionStore subscriptions();

    boolean registerEventListener(MetaStoreEventListener listener);
}
