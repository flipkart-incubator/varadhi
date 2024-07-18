package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;

public interface IamPolicyMetaStore {

    IamPolicyRecord getIamPolicyRecord(String authResourceId);

    void createIamPolicyRecord(IamPolicyRecord iamPolicyRecord);

    boolean isIamPolicyRecordPresent(String authResourceId);

    void updateIamPolicyRecord(IamPolicyRecord iamPolicyRecord);

    void deleteIamPolicyRecord(String authResourceId);
}
