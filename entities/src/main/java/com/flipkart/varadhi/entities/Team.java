package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Team name. Check naming constraints.")
public class Team extends MetaStoreEntity implements Validatable {

    @Setter
    private String org;

    private Team(String name, int version, String org) {
        super(name, version, MetaStoreEntityType.TEAM);
        this.org = org;
    }

    public static Team of(String name, String org) {
        return new Team(name, INITIAL_VERSION, org);
    }
}
