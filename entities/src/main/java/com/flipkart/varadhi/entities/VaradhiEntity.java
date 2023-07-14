package com.flipkart.varadhi.entities;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = AccessLevel.PRIVATE)
@Getter
public abstract class VaradhiEntity {

    final String name;

    @Setter
    private int version;

    protected VaradhiEntity(String name) {
        this.name = name;
    }
}
