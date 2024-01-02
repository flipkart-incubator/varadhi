package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
@ValidateMetaStoreEntity(message = "Invalid Org name. Check naming constraints.")
public class Org extends MetaStoreEntity {

    public Org(String name, int version) {
        super(name, version);
    }
}
