package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;

/**
 * Interface for storing and managing IAM policy records.
 * This interface provides methods for creating, updating, deleting, and checking
 * the existence of IAM policy records, as well as retrieving records
 * by authentication resource ID.
 */
public interface IamPolicyStore {

    interface Provider {
        IamPolicyStore iamPolicyMetaStore();
    }

    void create(IamPolicyRecord iamPolicyRecord);

    IamPolicyRecord get(String authResourceId);

    boolean exists(String authResourceId);

    void update(IamPolicyRecord iamPolicyRecord);

    void delete(String authResourceId);
}
