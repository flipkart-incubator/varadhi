package com.flipkart.varadhi.entities;

public interface KeyProvider {

    /*
    Provides unique key path identifying this instance and resource types.
    Can be used to lookup/identify this entity in persistent store.
    This should be hierarchical, like path of a file in filesystem.
     */
    String uniqueKeyPath();
}
