package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;

import java.util.List;

public interface IamPolicyMetaStore {
    List<IamPolicyRecord> getIamPolicyRecords();

    IamPolicyRecord getIamPolicyRecord(String authResourceId);

    void createIamPolicyRecord(IamPolicyRecord node);

    boolean isIamPolicyRecordPresent(String authResourceId);

    int updateIamPolicyRecord(IamPolicyRecord node);

    void deleteIamPolicyRecord(String authResourceId);
}
