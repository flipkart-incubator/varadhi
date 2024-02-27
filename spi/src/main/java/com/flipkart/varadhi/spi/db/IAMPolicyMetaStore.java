package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.IAMPolicyRecord;
import com.flipkart.varadhi.entities.auth.ResourceType;

import java.util.List;

public interface IAMPolicyMetaStore {
    List<IAMPolicyRecord> getIAMPolicyRecords();

    IAMPolicyRecord getIAMPolicyRecord(String authResourceId);

    void createIAMPolicyRecord(IAMPolicyRecord node);

    boolean isIAMPolicyRecordPresent(String authResourceId);

    int updateIAMPolicyRecord(IAMPolicyRecord node);

    void deleteIAMPolicyRecord(String authResourceId);
}
