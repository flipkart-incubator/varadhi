package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@EqualsAndHashCode (callSuper = true)
@FieldDefaults (makeFinal = true, level = AccessLevel.PRIVATE)
@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "@storageType")
public abstract class StorageTopic extends AbstractTopic {
    private final TopicCapacityPolicy capacity;

    public StorageTopic(String name, int version, TopicCapacityPolicy capacity) {
        super(name, version);
        this.capacity = capacity;
    }
}
