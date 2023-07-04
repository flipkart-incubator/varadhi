package com.flipkart.varadhi.entities;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@EqualsAndHashCode(callSuper = true)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public abstract class StorageTopic extends BaseTopic {
    public StorageTopic(InternalTopic.StorageKind kind, String name, int version) {
        super(name, kind, version);
    }
}
