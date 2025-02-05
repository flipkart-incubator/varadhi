package com.flipkart.varadhi.entities;

/**
 * Abstract class representing a topic entity in the MetaStore.
 * Extends the MetaStoreEntity class.
 */
public abstract class AbstractTopic extends MetaStoreEntity {

    /**
     * Constructor to initialize the AbstractTopic with a name and version.
     *
     * @param name    the name of the topic
     * @param version the version of the topic
     */
    protected AbstractTopic(String name, int version) {
        super(name, version);
    }
}
