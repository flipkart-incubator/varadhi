package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.StorageTopic;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@EqualsAndHashCode(callSuper = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class PulsarStorageTopic extends StorageTopic {
    int partitionCount;

    public PulsarStorageTopic(String name, int partitionCount) {
        super(name, Constants.INITIAL_VERSION);
        this.partitionCount = partitionCount;
    }

}
