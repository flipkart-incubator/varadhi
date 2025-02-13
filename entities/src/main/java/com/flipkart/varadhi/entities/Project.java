package com.flipkart.varadhi.entities;


import jakarta.validation.constraints.Size;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Project name. Check naming constraints.")
public class Project extends MetaStoreEntity implements Validatable {

    private String org;
    @Setter
    private String team;

    @Setter
    @Size (max = 100)
    private String description;

    private Project(String name, int version, String description, String team, String org) {
        super(name, version);
        this.description = description;
        this.team = team;
        this.org = org;
    }

    public static Project of(String name, String description, String team, String org) {
        return new Project(name, INITIAL_VERSION, description, team, org);
    }
}
