package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class Project extends VaradhiResource {
    String orgName;

    @Setter
    String teamName;

    @Setter
    String description;

    public Project(
            String name,
            int version,
            String description,
            String teamName,
            String orgName
    ) {
        super(name, version);
        this.description = description;
        this.teamName = teamName;
        this.orgName = orgName;
    }

}
