package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.auth.IAMPolicyRecord;
import com.flipkart.varadhi.entities.auth.ResourceType;

import java.util.List;

public interface IAMPolicyMetaStore {
    List<IAMPolicyRecord> getIAMPolicyRecords();

    IAMPolicyRecord getIAMPolicyRecord(String resourceIdWithType);

    IAMPolicyRecord getIAMPolicyRecord(ResourceType resourceType, String resourceId);

    void createIAMPolicyRecord(IAMPolicyRecord node);

    boolean isIAMPolicyRecordPresent(ResourceType resourceType, String resourceId);

    int updateIAMPolicyRecord(IAMPolicyRecord node);

    void deleteIAMPolicyRecord(ResourceType resourceType, String resourceId);
}
