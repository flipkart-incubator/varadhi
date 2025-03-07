package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;

/**
 * Interface for storing and managing IAM policy records.
 * This interface provides methods for creating, updating, deleting, and checking
 * the existence of IAM policy records, as well as retrieving records
 * by authentication resource ID.
 */
public interface IamPolicyMetaStore {

    void createIamPolicyRecord(IamPolicyRecord iamPolicyRecord);

    IamPolicyRecord getIamPolicyRecord(String authResourceId);

    boolean isIamPolicyRecordPresent(String authResourceId);

    void updateIamPolicyRecord(IamPolicyRecord iamPolicyRecord);

    void deleteIamPolicyRecord(String authResourceId);
}
