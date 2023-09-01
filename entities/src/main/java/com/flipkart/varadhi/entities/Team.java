package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.ValidateVaradhiResource;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;

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

    public Team cloneForCreate(String orgName) {
        return new Team(getName(), INITIAL_VERSION, orgName);
    }
}
