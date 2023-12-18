package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Team name. Check naming constraints.")
public class Team extends VaradhiResource {

    @Setter
    private String org;

    public Team(
            String name,
            int version,
            String org
    ) {
        super(name, version);
        this.org = org;
    }
}
