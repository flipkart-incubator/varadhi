package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public class MetaStoreEntity extends Versioned {

    protected MetaStoreEntity(String name, int version) {
        super(name, version);
    }
}
