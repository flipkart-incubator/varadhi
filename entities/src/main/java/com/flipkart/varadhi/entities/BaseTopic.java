package com.flipkart.varadhi.entities;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public abstract class BaseTopic {
    String name;
    InternalTopic.StorageKind kind;
}
