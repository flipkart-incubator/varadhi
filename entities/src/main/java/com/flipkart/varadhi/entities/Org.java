package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Org name. Check naming constraints.")
public class Org extends MetaStoreEntity implements Validatable {
    @JsonCreator
    Org(@JsonProperty ("name") String name, @JsonProperty ("version") int version) {
        super(name, version, MetaStoreEntityType.ORG);
    }

    public static Org of(String name) {
        return new Org(name, INITIAL_VERSION);
    }
}
