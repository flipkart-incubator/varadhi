package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class Team extends VaradhiResource {

    @Setter
    private String orgName;

    public Team(
            String name,
            int version,
            String orgName
    ) {
        super(name, version);
        this.orgName = orgName;
    }
}
