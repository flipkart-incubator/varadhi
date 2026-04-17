package com.flipkart.varadhi.spi.db;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, subscriptions, and regions.
 */
public interface MetaStore {

    OrgStore orgs();

    TeamStore teams();

    ProjectStore projects();

    TopicStore topics();

    SubscriptionStore subscriptions();

    RegionStore regions();

    boolean registerEventListener(MetaStoreEventListener listener);
}
