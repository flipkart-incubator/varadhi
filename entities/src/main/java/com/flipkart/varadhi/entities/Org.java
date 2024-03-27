package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
@ValidateResource(message = "Invalid Org name. Check naming constraints.")
public class Org extends MetaStoreEntity implements Validatable {

    public Org(String name, int version) {
        super(name, version);
    }
}
