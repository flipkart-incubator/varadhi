package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Org name. Check naming constraints.")
public class Org extends MetaStoreEntity implements Validatable {
    private Org(String name, int version) {
        super(name, version);
    }

    public static Org of(String name) {
        return new Org(name, INITIAL_VERSION);
    }
}
