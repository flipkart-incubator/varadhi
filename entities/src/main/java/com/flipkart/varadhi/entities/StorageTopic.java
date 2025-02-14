package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@EqualsAndHashCode
@FieldDefaults (makeFinal = true, level = AccessLevel.PRIVATE)
@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "@storageType")
public abstract class StorageTopic implements AbstractTopic {
    String name;
    TopicCapacityPolicy capacity;

    protected StorageTopic(String name, TopicCapacityPolicy capacity) {
        this.name = name;
        this.capacity = capacity;
    }

    @Override
    public String getName() {
        return name;
    }
}
